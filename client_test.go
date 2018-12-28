/*
Copyright 2018 github.com/ucirello

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pglock_test

import (
	"bytes"
	"context"
	"database/sql"
	"flag"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"

	"cirello.io/errors"
	"cirello.io/pglock"
	"github.com/lib/pq"
)

var dsn = flag.String("dsn", "postgres://postgres@localhost/postgres?sslmode=disable", "connection string to the test database server")

func init() {
	flag.Parse()
	db, err := sql.Open("postgres", *dsn)
	if err != nil {
		log.Fatal("cannot connect to test database server:", err)
	}
	c, err := pglock.New(db)
	if err != nil {
		log.Fatal(err)
	}
	_ = c.CreateTable()
}

func TestNew(t *testing.T) {
	t.Parallel()
	if _, err := pglock.New(nil); err == nil {
		t.Error("bad driver should trigger a pglock.ErrNotPostgreSQLDriver")
	}
	// Skipping driver type assertion so to avoid importing another DB
	// driver just for this particular case.
	db, err := sql.Open("postgres", *dsn)
	if err != nil {
		t.Fatal("cannot connect to test database server:", err)
	}
	if _, err := pglock.New(db, pglock.WithLeaseDuration(time.Second), pglock.WithHeartbeatFrequency(time.Second)); err != pglock.ErrDurationTooSmall {
		t.Fatal("got unexpected error when the client was misconfigured")
	}
}

func TestFailIfLocked(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("postgres", *dsn)
	if err != nil {
		t.Fatal("cannot connect to test database server:", err)
	}
	name := randStr(32)
	c, err := pglock.New(
		db,
		pglock.WithLogger(&testLogger{t}),
		pglock.WithLeaseDuration(5*time.Second),
		pglock.WithHeartbeatFrequency(1*time.Second),
	)
	if err != nil {
		t.Fatal("cannot create lock client:", err)
	}
	l, err := c.Acquire(name)
	if err != nil {
		t.Fatal("unexpected error while acquiring lock:", err)
	}
	defer l.Close()
	t.Log("first lock acquired")
	if _, err := c.Acquire(name, pglock.FailIfLocked()); err != pglock.ErrNotAcquired {
		t.Fatal("expected ErrNotAcquired")
	}
}

func TestKeepOnRelease(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("postgres", *dsn)
	if err != nil {
		t.Fatal("cannot connect to test database server:", err)
	}
	name := randStr(32)
	c, err := pglock.New(
		db,
		pglock.WithLogger(&testLogger{t}),
		pglock.WithLeaseDuration(5*time.Second),
		pglock.WithHeartbeatFrequency(1*time.Second),
	)
	if err != nil {
		t.Fatal("cannot create lock client:", err)
	}
	expected := []byte("42")
	l, err := c.Acquire(name, pglock.KeepOnRelease(), pglock.WithData(expected))
	if err != nil {
		t.Fatal("unexpected error while acquiring lock:", err)
	}
	t.Log("lock acquired")
	l.Close()

	l2, err := c.Acquire(name)
	if err != nil {
		t.Fatal("unexpected error while acquiring lock:", err)
	}
	t.Log("lock reacquired")
	defer l.Close()
	if !bytes.Equal(l2.Data(), expected) {
		t.Fatal("lock content lost")
	}
}

func TestClose(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("postgres", *dsn)
	if err != nil {
		t.Fatal("cannot connect to test database server:", err)
	}
	name := randStr(32)
	c, err := pglock.New(
		db,
		pglock.WithLogger(&testLogger{t}),
		pglock.WithLeaseDuration(5*time.Second),
		pglock.WithHeartbeatFrequency(1*time.Second),
	)
	if err != nil {
		t.Fatal("cannot create lock client:", err)
	}
	l, err := c.Acquire(name)
	if err != nil {
		t.Fatal("unexpected error while acquiring lock:", err)
	}
	t.Log("lock acquired")
	l.Close()
	if err := l.Close(); err != nil && err != pglock.ErrLockAlreadyReleased {
		t.Fatal("close not idempotent - second lock release should always work:", err)
	}
}

func TestAcquire(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("postgres", *dsn)
	if err != nil {
		t.Fatal("cannot connect to test database server:", err)
	}
	name := randStr(32)
	c, err := pglock.New(
		db,
		pglock.WithLogger(&testLogger{t}),
		pglock.WithLeaseDuration(5*time.Second),
		pglock.WithHeartbeatFrequency(1*time.Second),
	)
	if err != nil {
		t.Fatal("cannot create lock client:", err)
	}
	l1, err := c.Acquire(name)
	if err != nil {
		t.Fatal("unexpected error while acquiring 1st lock:", err)
	}
	t.Log("acquired first lock")
	var wg sync.WaitGroup
	wg.Add(1)
	var locked bool
	go func() {
		defer wg.Done()
		l2, err := c.Acquire(name)
		if err != nil {
			t.Fatal("unexpected error while acquiring 2nd lock:", err)
		}
		t.Log("acquired second lock")
		locked = true
		l2.Close()
	}()
	time.Sleep(6 * time.Second)
	l1.Close()
	wg.Wait()
	if !locked {
		t.Fatal("concurrent lock flow is not working")
	}
}

func TestGet(t *testing.T) {
	t.Parallel()
	t.Run("happy path", func(t *testing.T) {
		db, err := sql.Open("postgres", *dsn)
		if err != nil {
			t.Fatal("cannot connect to test database server:", err)
		}
		name := randStr(32)
		c, err := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
		)
		if err != nil {
			t.Fatal("cannot create lock client:", err)
		}
		expected := []byte("42")
		l, err := c.Acquire(name, pglock.WithData(expected))
		if err != nil {
			t.Fatal("unexpected error while acquiring lock:", err)
		}
		defer l.Close()

		got, err := c.GetData(name)
		if err != nil {
			t.Fatal("cannot load data from the lock entry:", err)
		} else if !bytes.Equal(got, expected) {
			t.Fatal("corrupted data load from the lock entry:", got)
		}
	})
	t.Run("unknown key", func(t *testing.T) {
		db, err := sql.Open("postgres", *dsn)
		if err != nil {
			t.Fatal("cannot connect to test database server:", err)
		}
		name := "lock-404"
		c, err := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
		)
		if err != nil {
			t.Fatal("cannot create lock client:", err)
		}
		if _, err := c.GetData(name); err == nil {
			t.Fatal("expected error not found on loading unknown key")
		} else if !errors.Is(errors.NotExist, err) {
			t.Fatal("unexpected error kind found on loading unknown key:", err)
		} else if err != pglock.ErrLockNotFound {
			t.Fatal("unexpected error found on loading unknown key:", err)
		}
	})
}

func TestLockData(t *testing.T) {
	t.Parallel()
	t.Run("reuse lock data", func(t *testing.T) {
		db, err := sql.Open("postgres", *dsn)
		if err != nil {
			t.Fatal("cannot connect to test database server:", err)
		}
		name := randStr(32)
		c, err := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
		)
		if err != nil {
			t.Fatal("cannot create lock client:", err)
		}
		expected := []byte("42")
		l1, err := c.Acquire(name, pglock.WithData(expected))
		if err != nil {
			t.Fatal("unexpected error while acquiring lock:", err)
		}
		l1.Close()
		t.Log("first lock stored")

		l2, err := c.Acquire(name)
		if err != nil {
			t.Fatal("unexpected error while acquiring lock:", err)
		}
		defer l2.Close()
		t.Log("second lock stored")

		if !bytes.Equal(l2.Data(), expected) {
			t.Log("lost data when lock was recycled")
		}
	})

	t.Run("replace lock data", func(t *testing.T) {
		db, err := sql.Open("postgres", *dsn)
		if err != nil {
			t.Fatal("cannot connect to test database server:", err)
		}
		name := randStr(32)
		c, err := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
		)
		if err != nil {
			t.Fatal("cannot create lock client:", err)
		}

		l1, err := c.Acquire(name, pglock.WithData([]byte("original")))
		if err != nil {
			t.Fatal("unexpected error while acquiring lock:", err)
		}
		l1.Close()
		t.Log("first lock stored")

		expected := []byte("42")
		l2, err := c.Acquire(name, pglock.WithData(expected), pglock.ReplaceData())
		if err != nil {
			t.Fatal("unexpected error while acquiring lock:", err)
		}
		defer l2.Close()
		t.Log("second lock stored")

		if !bytes.Equal(l2.Data(), expected) {
			t.Log("lost data when lock was recycled")
		}
	})
}

func TestCustomTable(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("postgres", *dsn)
	if err != nil {
		t.Fatal("cannot connect to test database server:", err)
	}
	tableName := randStr(32)
	name := randStr(32)
	c, err := pglock.New(
		db,
		pglock.WithLogger(&testLogger{t}),
		pglock.WithCustomTable(tableName),
	)
	if err != nil {
		t.Fatal("cannot create lock client:", err)
	}
	if err := c.CreateTable(); err != nil {
		t.Fatal("cannot create table:", err)
	}
	defer func() {
		db.Exec("DROP TABLE " + tableName)
	}()

	l1, err := c.Acquire(name)
	if err != nil {
		t.Fatal("unexpected error while acquiring lock:", err)
	}
	defer l1.Close()
	t.Log("first lock stored")
}

func TestCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	db, err := sql.Open("postgres", *dsn)
	if err != nil {
		t.Fatal("cannot connect to test database server:", err)
	}
	name := randStr(32)
	c, err := pglock.New(
		db,
		pglock.WithLogger(&testLogger{t}),
	)
	if err != nil {
		t.Fatal("cannot create lock client:", err)
	}
	if _, err := c.AcquireContext(ctx, name); err != pglock.ErrNotAcquired {
		t.Fatal("canceled context should not be able to acquire locks")
	}
}

func TestDo(t *testing.T) {
	t.Parallel()

	t.Run("lost lock", func(t *testing.T) {
		db, err := sql.Open("postgres", *dsn)
		if err != nil {
			t.Fatal("cannot connect to test database server:", err)
		}
		name := randStr(32)
		c, err := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
			pglock.WithLeaseDuration(5*time.Second),
			pglock.WithHeartbeatFrequency(1*time.Second),
		)
		if err != nil {
			t.Fatal("cannot create lock client:", err)
		}
		ranOnce := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			err = c.Do(context.Background(), name, func(ctx context.Context, l *pglock.Lock) error {
				once := make(chan struct{}, 1)
				once <- struct{}{}
				for {
					select {
					case <-ctx.Done():
						t.Log("context canceled")
						return ctx.Err()
					case <-once:
						t.Log("executed once")
						close(ranOnce)
					}
				}
			})
			if err != nil && err != context.Canceled {
				t.Fatal("unexpected error while running under lock:", err)
			}
		}()
		<-ranOnce
		t.Log("directly releasing lock")
		if err := releaseLockByName(db, name); err != nil {
			t.Fatal("cannot forcefully release lock")
		}
		wg.Wait()
	})

	t.Run("normally completed", func(t *testing.T) {
		db, err := sql.Open("postgres", *dsn)
		if err != nil {
			t.Fatal("cannot connect to test database server:", err)
		}
		name := randStr(32)
		c, err := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
			pglock.WithLeaseDuration(5*time.Second),
			pglock.WithHeartbeatFrequency(1*time.Second),
		)
		if err != nil {
			t.Fatal("cannot create lock client:", err)
		}
		err = c.Do(context.Background(), name, func(ctx context.Context, l *pglock.Lock) error {
			for i := 0; i < 5; i++ {
				t.Log("i = ", i)
				time.Sleep(1 * time.Second)
			}
			return nil
		})
		if err != nil && err != context.Canceled {
			t.Fatal("unexpected error while running under lock:", err)
		}
	})
}

func releaseLockByName(db *sql.DB, name string) error {
	const serializationErrorCode = "40001"
	for {
		_, err := db.Exec("UPDATE locks SET record_version_number = NULL WHERE name = $1", name)
		if e, ok := err.(*pq.Error); ok {
			if e.Code == serializationErrorCode {
				continue
			}
		}
		return errors.E(err, "cannot release lock by name")
	}
}

var chars = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func init() {
	rand.Seed(time.Now().UnixNano())
}

func randStr(n int) string {
	var b bytes.Buffer
	for i := 0; i < n; i++ {
		b.WriteByte(chars[rand.Intn(len(chars))])
	}
	return b.String()
}

type testLogger struct {
	t *testing.T
}

func (t *testLogger) Println(v ...interface{}) {
	t.t.Log(v...)
}
