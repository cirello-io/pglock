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
	"database/sql/driver"
	"flag"
	"sync"
	"testing"
	"time"

	"cirello.io/pglock"
	"github.com/lib/pq"
	"golang.org/x/xerrors"
)

type fakeDriver struct{}

func (fd *fakeDriver) Open(string) (driver.Conn, error) {
	return nil, nil
}

func init() {
	sql.Register("fakeDriver", &fakeDriver{})
}

var dsn = flag.String("dsn", "postgres://postgres@localhost/postgres?sslmode=disable", "connection string to the test database server")

func setupDB(t *testing.T) *sql.DB {
	db, err := sql.Open("postgres", *dsn)
	if err != nil {
		t.Fatal("cannot connect to test database server:", err)
	}
	c, err := pglock.New(db)
	if err != nil {
		t.Fatal("cannot connect:", err)
	}
	_ = c.CreateTable()
	return db
}

func TestNew(t *testing.T) {
	t.Parallel()
	// Skipping driver type assertion so to avoid importing another DB
	// driver just for this particular case.
	t.Run("nil driver", func(t *testing.T) {
		if _, err := pglock.New(nil); err == nil {
			t.Error("bad driver should trigger a pglock.ErrNotPostgreSQLDriver")
		}
	})
	t.Run("nil driver at unsafe", func(t *testing.T) {
		if _, err := pglock.UnsafeNew(nil); err == nil {
			t.Error("bad driver should trigger a pglock.ErrNotPostgreSQLDriver")
		}
	})
	t.Run("bad driver", func(t *testing.T) {
		db, err := sql.Open("fakeDriver", *dsn)
		if err != nil {
			t.Fatal("cannot connect to test database server:", err)
		}
		if _, err := pglock.New(db, pglock.WithLeaseDuration(time.Second), pglock.WithHeartbeatFrequency(time.Second)); err != pglock.ErrNotPostgreSQLDriver {
			t.Fatal("got unexpected error when the client was fed with a bad driver:", err)
		}
	})
	t.Run("good driver", func(t *testing.T) {
		db := setupDB(t)
		defer db.Close()
		if _, err := pglock.New(db, pglock.WithLeaseDuration(time.Second), pglock.WithHeartbeatFrequency(time.Second)); err != pglock.ErrDurationTooSmall {
			t.Fatal("got unexpected error when the client was misconfigured:", err)
		}
	})
	t.Run("bad driver at unsafe", func(t *testing.T) {
		db, err := sql.Open("fakeDriver", *dsn)
		if err != nil {
			t.Fatal("cannot connect to test database server:", err)
		}
		if _, err := pglock.UnsafeNew(db, pglock.WithLeaseDuration(time.Second), pglock.WithHeartbeatFrequency(time.Second)); err != pglock.ErrDurationTooSmall {
			t.Fatal("got unexpected error when the client was misconfigured:", err)
		}
	})
}

func TestOpen(t *testing.T) {
	t.Parallel()
	t.Run("good dsn", func(t *testing.T) {
		db := setupDB(t)
		defer db.Close()
		if _, err := pglock.New(db, pglock.WithLeaseDuration(time.Second), pglock.WithHeartbeatFrequency(time.Second)); err != pglock.ErrDurationTooSmall {
			t.Fatal("got unexpected error when the client was misconfigured")
		}
	})
}

func TestFailIfLocked(t *testing.T) {
	t.Parallel()
	db := setupDB(t)
	defer db.Close()
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

func TestCustomHeartbeatContext(t *testing.T) {
	t.Parallel()
	t.Run("custom context", func(t *testing.T) {
		db := setupDB(t)
		defer db.Close()
		name := randStr(32)
		const heartbeatFrequency = 2 * time.Second
		c, err := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
			pglock.WithLeaseDuration(heartbeatFrequency*3),
			pglock.WithHeartbeatFrequency(heartbeatFrequency),
		)
		if err != nil {
			t.Fatal("cannot create lock client:", err)
		}
		hbCtx, hbCancel := context.WithCancel(context.Background())
		l, err := c.Acquire(name, pglock.WithCustomHeartbeatContext(hbCtx))
		if err != nil {
			t.Fatal("unexpected error while acquiring lock:", err)
		}
		defer l.Close()
		originalRVN := l.RecordVersionNumber()
		hbCancel()
		time.Sleep(time.Second + heartbeatFrequency)
		newRVN := l.RecordVersionNumber()
		t.Log("rvn", originalRVN, newRVN)
		if originalRVN != newRVN {
			t.Fatal("heartbeat did not stop after cancel")
		}
	})
	t.Run("inherited context", func(t *testing.T) {
		db := setupDB(t)
		defer db.Close()
		name := randStr(32)
		const heartbeatFrequency = 2 * time.Second
		c, err := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
			pglock.WithLeaseDuration(heartbeatFrequency*3),
			pglock.WithHeartbeatFrequency(heartbeatFrequency),
		)
		if err != nil {
			t.Fatal("cannot create lock client:", err)
		}
		l, err := c.Acquire(name)
		if err != nil {
			t.Fatal("unexpected error while acquiring lock:", err)
		}
		defer l.Close()
		originalRVN := l.RecordVersionNumber()
		time.Sleep(time.Second + heartbeatFrequency)
		newRVN := l.RecordVersionNumber()
		t.Log("rvn", originalRVN, newRVN)
		if originalRVN == newRVN {
			t.Fatal("heartbeat did not run")
		}
	})
}

func TestKeepOnRelease(t *testing.T) {
	t.Parallel()
	db := setupDB(t)
	defer db.Close()
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
	db := setupDB(t)
	defer db.Close()
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
	db := setupDB(t)
	defer db.Close()
	name := randStr(32)
	const heartbeatFrequency = 1 * time.Second
	c, err := pglock.New(
		db,
		pglock.WithLogger(&testLogger{t}),
		pglock.WithLeaseDuration(5*time.Second),
		pglock.WithHeartbeatFrequency(heartbeatFrequency),
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
	db := setupDB(t)
	defer db.Close()
	t.Run("happy path - data", func(t *testing.T) {
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
	t.Run("happy path - lock", func(t *testing.T) {
		name := randStr(32)
		const expectedOwner = "custom-owner"
		c, err := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
			pglock.WithOwner(expectedOwner),
		)
		if err != nil {
			t.Fatal("cannot create lock client:", err)
		}
		expectedData := []byte("42")
		l, err := c.Acquire(name, pglock.WithData(expectedData))
		if err != nil {
			t.Fatal("unexpected error while acquiring lock:", err)
		}
		defer l.Close()

		got, err := c.Get(name)
		if err != nil {
			t.Fatal("cannot load data from the lock entry:", err)
		} else if gotData := got.Data(); !bytes.Equal(gotData, expectedData) {
			t.Fatal("corrupted load data from the lock entry:", got)
		} else if gotOwner := got.Owner(); gotOwner != expectedOwner {
			t.Fatal("corrupted owner from the lock entry:", got)
		}
	})
	t.Run("unknown key - data", func(t *testing.T) {
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
		} else if notFound := (&pglock.NotExistError{}); !xerrors.As(err, &notFound) {
			t.Fatal("unexpected error kind found on loading unknown key:", err)
		} else if err != pglock.ErrLockNotFound {
			t.Fatal("unexpected error found on loading unknown key:", err)
		}
	})
}

func TestLockData(t *testing.T) {
	t.Parallel()
	db := setupDB(t)
	defer db.Close()
	t.Run("reuse lock data", func(t *testing.T) {
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
	db := setupDB(t)
	defer db.Close()
	t.Run("happy path", func(t *testing.T) {
		tableName := randStr(32)
		defer func() {
			db.Exec("DROP TABLE " + tableName)
		}()
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
		l1, err := c.Acquire(name)
		if err != nil {
			t.Fatal("unexpected error while acquiring lock:", err)
		}
		defer l1.Close()
		t.Log("first lock stored")
	})
	t.Run("duplicated call", func(t *testing.T) {
		tableName := randStr(32)
		defer func() {
			db.Exec("DROP TABLE " + tableName)
		}()
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
		if err := c.CreateTable(); err == nil {
			t.Fatal("expected error not found")
		}
	})
}

func TestCanceledContext(t *testing.T) {
	db := setupDB(t)
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
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
	db := setupDB(t)
	defer db.Close()
	t.Run("lost lock", func(t *testing.T) {
		const heartbeatFrequency = 1 * time.Second
		name := randStr(32)
		c, err := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
			pglock.WithLeaseDuration(5*time.Second),
			pglock.WithHeartbeatFrequency(heartbeatFrequency),
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
			t.Fatalf("cannot forcefully release lock: %v", err)
		}
		wg.Wait()
	})

	t.Run("normally completed", func(t *testing.T) {
		const heartbeatFrequency = 1 * time.Second
		name := randStr(32)
		c, err := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
			pglock.WithLeaseDuration(5*time.Second),
			pglock.WithHeartbeatFrequency(heartbeatFrequency),
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

	t.Run("canceled context", func(t *testing.T) {
		const heartbeatFrequency = 1 * time.Second
		name := randStr(32)
		c, err := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
			pglock.WithLeaseDuration(5*time.Second),
			pglock.WithHeartbeatFrequency(heartbeatFrequency),
		)
		if err != nil {
			t.Fatal("cannot create lock client:", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err = c.Do(ctx, name, func(ctx context.Context, l *pglock.Lock) error {
			return nil
		})
		if err != pglock.ErrNotAcquired {
			t.Fatal("unexpected error while running under lock with canceled context:", err)
		}
	})

	t.Run("canceled heartbeat context", func(t *testing.T) {
		const heartbeatFrequency = 1 * time.Second
		name := randStr(32)
		c, err := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
			pglock.WithLeaseDuration(5*time.Second),
			pglock.WithHeartbeatFrequency(heartbeatFrequency),
		)
		if err != nil {
			t.Fatal("cannot create lock client:", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		err = c.Do(context.Background(), name, func(ctx context.Context, l *pglock.Lock) error {
			for {
				if err := ctx.Err(); err != nil {
					return err
				}
				t.Log("ping")
				time.Sleep(2 * heartbeatFrequency)
			}
		}, pglock.WithCustomHeartbeatContext(ctx))
		if err != nil && err != context.Canceled {
			t.Fatal("unexpected error while running under lock:", err)
		}
		t.Log("done")
	})

	t.Run("handle failIfLocked", func(t *testing.T) {
		name := randStr(32)
		c, err := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
			pglock.WithLeaseDuration(5*time.Second),
			pglock.WithHeartbeatFrequency(0),
		)
		if err != nil {
			t.Fatal("cannot create lock client:", err)
		}
		if _, err := c.Acquire(name); err != nil {
			t.Fatal("cannot grab lock:", err)
		}
		err = c.Do(context.Background(), name, func(ctx context.Context, l *pglock.Lock) error {
			return xerrors.New("should not have been executed")
		}, pglock.FailIfLocked())
		if err != pglock.ErrNotAcquired {
			t.Fatal("unexpected error while running under lock:", err)
		}
		if _, err := c.Acquire(name); err != nil {
			t.Fatal("cannot grab lock:", err)
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
		t.Log("done")
	})
}

func TestOwner(t *testing.T) {
	t.Parallel()
	db := setupDB(t)
	defer db.Close()
	const expectedOwner = "custom-owner"
	lockName := randStr(32)
	c, err := pglock.New(
		db,
		pglock.WithLogger(&testLogger{t}),
		pglock.WithLeaseDuration(5*time.Second),
		pglock.WithHeartbeatFrequency(1*time.Second),
		pglock.WithOwner(expectedOwner),
	)
	if err != nil {
		t.Fatal("cannot create lock client:", err)
	}
	l1, err := c.Acquire(lockName)
	if err != nil {
		t.Fatal("unexpected error while acquiring 1st lock:", err)
	}
	defer l1.Close()
	if owner := l1.Owner(); owner != expectedOwner {
		t.Fatalf("owner not stored in the acquired lock: %s", owner)
	}
	row := db.QueryRow("SELECT owner FROM "+pglock.DefaultTableName+" WHERE name = $1", lockName)
	var foundOwner string
	if err := row.Scan(&foundOwner); err != nil {
		t.Fatalf("cannot load owner from the database: %v", err)
	}
	if foundOwner != expectedOwner {
		t.Fatalf("owner not stored in the lock table: %s", foundOwner)
	}
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
		if err != nil {
			return xerrors.Errorf("cannot release lock by name: %v", err)
		}
		return nil
	}
}

func TestSendHeartbeat(t *testing.T) {
	db := setupDB(t)
	defer db.Close()
	t.Run("bad sendHeartbeat", func(t *testing.T) {
		c, err := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
			pglock.WithLeaseDuration(5*time.Second),
			pglock.WithHeartbeatFrequency(0),
		)
		if err != nil {
			t.Fatal("cannot create lock client:", err)
		}
		name := randStr(32)
		l, err := c.Acquire(name)
		if err != nil {
			t.Fatal("unexpected error while acquiring lock:", err)
		}
		if err := c.Release(l); err != nil {
			t.Fatal("unexpected error while releasing lock:", err)
		}
		err = c.SendHeartbeat(context.Background(), l)
		if err == nil {
			t.Error("sendHeartbeat error missing")
		}
		t.Log(err)
	})
	t.Run("good sendHeartbeat", func(t *testing.T) {
		c, err := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
			pglock.WithLeaseDuration(5*time.Second),
			pglock.WithHeartbeatFrequency(0),
		)
		if err != nil {
			t.Fatal("cannot create lock client:", err)
		}
		name := randStr(32)
		l, err := c.Acquire(name)
		if err != nil {
			t.Fatal("unexpected error while acquiring lock:", err)
		}
		t.Log("RVN:", l.RecordVersionNumber())
		err = c.SendHeartbeat(context.Background(), l)
		if err != nil {
			t.Errorf("sendHeartbeat failed: %v", err)
		}
		t.Log("RVN:", l.RecordVersionNumber())
		if err := c.Release(l); err != nil {
			t.Fatal("unexpected error while releasing lock:", err)
		}
	})
	t.Run("racy", testSendHeartbeatRacy)
}

func testSendHeartbeatRacy(t *testing.T) {
	db := setupDB(t)
	defer db.Close()
	c, err := pglock.New(
		db,
		pglock.WithLogger(&testLogger{t}),
		pglock.WithLeaseDuration(5*time.Second),
		pglock.WithHeartbeatFrequency(1),
	)
	if err != nil {
		t.Fatal("cannot create lock client:", err)
	}
	name := randStr(32)
	l, err := c.Acquire(name)
	if err != nil {
		t.Fatal("unexpected error while acquiring lock:", err)
	}

	var wg sync.WaitGroup
	done := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := c.Release(l); err != nil {
			t.Fatal("unexpected error while releasing lock:", err)
		} else {
			close(done)
		}
	}()
	select {
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock between sendHeartbeat and release")
	case <-done:
	}
	wg.Wait()
}
