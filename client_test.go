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
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"cirello.io/pglock"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/lib/pq"
	"golang.org/x/sync/errgroup"
)

type fakeDriver struct{}

func (fd *fakeDriver) Open(string) (driver.Conn, error) {
	return nil, nil
}

func init() {
	sql.Register("fakeDriver", &fakeDriver{})
}

var dsn = flag.String("dsn", "postgres://postgres@localhost/postgres?sslmode=disable", "connection string to the test database server")

func setupDBConn(tb testing.TB) *sql.DB {
	tb.Helper()
	db, err := sql.Open("postgres", *dsn)
	if err != nil {
		tb.Fatal("cannot connect to test database server:", err)
	}
	return db
}

func setupDB(tb testing.TB, options ...pglock.ClientOption) (*sql.DB, string) {
	tb.Helper()
	tableName := randStr()
	db, err := sql.Open("postgres", *dsn)
	if err != nil {
		tb.Fatal("cannot connect to test database server:", err)
	}
	c, err := pglock.New(db, append([]pglock.ClientOption{pglock.WithCustomTable(tableName)}, options...)...)
	if err != nil {
		tb.Fatal("cannot connect:", err)
	}
	if err := c.TryCreateTable(); err != nil {
		tb.Fatal("attempt to create table failed:", err)
	}
	return db, tableName
}

func setupCustomDB(t *testing.T, driver string, options ...pglock.ClientOption) (*sql.DB, string) {
	t.Helper()
	tableName := randStr()
	db, err := sql.Open(driver, *dsn)
	if err != nil {
		t.Fatal("cannot connect to test database server:", err)
	}
	c, err := pglock.UnsafeNew(db, append([]pglock.ClientOption{pglock.WithCustomTable(tableName)}, options...)...)
	if err != nil {
		t.Fatal("cannot connect:", err)
	}
	_ = c.CreateTable()
	return db, tableName
}

func tableInDB(db *sql.DB, tableName string) (bool, error) {
	var count int
	countQuery := `SELECT COUNT(*)
		FROM INFORMATION_SCHEMA.TABLES
		where table_name = '` + tableName + `'`
	row := db.QueryRow(countQuery)
	err := row.Scan(&count)
	if err != nil {
		return false, err
	}

	if count < 0 || 1 < count {
		return false, fmt.Errorf("expected count to be 0 or 1")
	}

	return count == 1, nil
}

func TestDropTable(t *testing.T) {
	db, _ := setupDB(t, pglock.WithCustomTable(pglock.DefaultTableName))
	defer db.Close()
	t.Run("default name", func(t *testing.T) {
		c, err := pglock.New(db)
		if err != nil {
			t.Fatal("cannot connect:", err)
		}

		tableName := pglock.DefaultTableName
		exist, err := tableInDB(db, tableName)
		if err != nil {
			t.Fatal("error checking if table exists")
		}
		if !exist {
			t.Fatal("table with name " + tableName + " not found")
		}

		err = c.DropTable()
		if err != nil {
			t.Error("error cleaning up database")
		}

		exist, err = tableInDB(db, tableName)
		if err != nil {
			t.Fatal("error checking if table exists")
		}
		if exist {
			t.Error("table with name " + tableName + " still exists")
		}
	})

	t.Run("custom tablename", func(t *testing.T) {
		tableName := randStr()
		defer func() {
			_, _ = db.Exec("DROP TABLE " + tableName)
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

		err = c.DropTable()
		if err != nil {
			t.Error("error cleaning up database")
		}

		exist, err := tableInDB(db, tableName)
		if err != nil {
			t.Fatal("error checking if table exists")
		}
		if exist {
			t.Error("table with name " + tableName + " still exists")
		}
	})

	t.Run("table does not exist", func(t *testing.T) {
		tableName := randStr()

		c, _ := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
			pglock.WithCustomTable(tableName),
		)

		exist, _ := tableInDB(db, tableName)
		if exist {
			t.Fatal("table with name " + tableName + " already exists")
		}

		err := c.DropTable()
		if err == nil {
			t.Error("did not receive an error dropping a table that does not exist")
		}
	})
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
		if _, err := pglock.New(db, pglock.WithLeaseDuration(time.Second), pglock.WithHeartbeatFrequency(time.Second)); !errors.Is(err, pglock.ErrNotPostgreSQLDriver) {
			t.Fatal("got unexpected error when the client was fed with a bad driver:", err)
		}
	})
	t.Run("good driver", func(t *testing.T) {
		db := setupDBConn(t)
		defer db.Close()
		if _, err := pglock.New(db, pglock.WithLeaseDuration(time.Second), pglock.WithHeartbeatFrequency(time.Second), pglock.WithCustomTable(randStr())); !errors.Is(err, pglock.ErrDurationTooSmall) {
			t.Fatal("got unexpected error when the client was misconfigured:", err)
		}
	})
	t.Run("bad driver at unsafe", func(t *testing.T) {
		db, err := sql.Open("fakeDriver", *dsn)
		if err != nil {
			t.Fatal("cannot connect to test database server:", err)
		}
		if _, err := pglock.UnsafeNew(db, pglock.WithLeaseDuration(time.Second), pglock.WithHeartbeatFrequency(time.Second)); !errors.Is(err, pglock.ErrDurationTooSmall) {
			t.Fatal("got unexpected error when the client was misconfigured:", err)
		}
	})
}

func TestOpen(t *testing.T) {
	t.Parallel()
	t.Run("good dsn", func(t *testing.T) {
		db, tableName := setupDB(t)
		defer db.Close()
		if _, err := pglock.New(db, pglock.WithLeaseDuration(time.Second), pglock.WithHeartbeatFrequency(time.Second), pglock.WithCustomTable(tableName)); !errors.Is(err, pglock.ErrDurationTooSmall) {
			t.Fatal("got unexpected error when the client was misconfigured")
		}
	})

}

func TestFailIfLocked(t *testing.T) {
	t.Parallel()
	db, tableName := setupDB(t)
	defer db.Close()
	name := randStr()
	c, err := pglock.New(
		db,
		pglock.WithLogger(&testLogger{t}),
		pglock.WithLeaseDuration(5*time.Second),
		pglock.WithHeartbeatFrequency(1*time.Second),
		pglock.WithCustomTable(tableName),
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
	if _, err := c.Acquire(name, pglock.FailIfLocked()); !errors.Is(err, pglock.ErrNotAcquired) {
		t.Fatal("expected ErrNotAcquired")
	}
}

func TestCustomHeartbeatContext(t *testing.T) {
	t.Parallel()
	t.Run("custom context", func(t *testing.T) {
		db, tableName := setupDB(t)
		defer db.Close()
		name := randStr()
		const heartbeatFrequency = 2 * time.Second
		c, err := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
			pglock.WithLeaseDuration(heartbeatFrequency*3),
			pglock.WithHeartbeatFrequency(heartbeatFrequency),
			pglock.WithCustomTable(tableName),
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
		db, tableName := setupDB(t)
		defer db.Close()
		name := randStr()
		const heartbeatFrequency = 2 * time.Second
		c, err := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
			pglock.WithLeaseDuration(heartbeatFrequency*3),
			pglock.WithHeartbeatFrequency(heartbeatFrequency),
			pglock.WithCustomTable(tableName),
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
	db, tableName := setupDB(t)
	defer db.Close()
	name := randStr()
	c, err := pglock.New(
		db,
		pglock.WithLogger(&testLogger{t}),
		pglock.WithLeaseDuration(5*time.Second),
		pglock.WithHeartbeatFrequency(1*time.Second),
		pglock.WithCustomTable(tableName),
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
	defer l2.Close()
	t.Log("lock reacquired")
	defer l.Close()
	if !bytes.Equal(l2.Data(), expected) {
		t.Fatal("lock content lost")
	}
}

func TestClose(t *testing.T) {
	t.Parallel()
	db, tableName := setupDB(t)
	defer db.Close()
	name := randStr()
	c, err := pglock.New(
		db,
		pglock.WithLogger(&testLogger{t}),
		pglock.WithLeaseDuration(5*time.Second),
		pglock.WithHeartbeatFrequency(1*time.Second),
		pglock.WithCustomTable(tableName),
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
	if err := l.Close(); err != nil && !errors.Is(err, pglock.ErrLockAlreadyReleased) {
		t.Fatal("close not idempotent - second lock release should always work:", err)
	}
}

func TestAcquire(t *testing.T) {
	t.Parallel()
	db, tableName := setupDB(t)
	defer db.Close()
	name := randStr()
	const heartbeatFrequency = 1 * time.Second
	c, err := pglock.New(
		db,
		pglock.WithLogger(&testLogger{t}),
		pglock.WithLeaseDuration(5*time.Second),
		pglock.WithHeartbeatFrequency(heartbeatFrequency),
		pglock.WithCustomTable(tableName),
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
	var err2 error
	go func() {
		defer wg.Done()
		l2, err := c.Acquire(name)
		if err != nil {
			err2 = err
			return
		}
		t.Log("acquired second lock")
		locked = true
		l2.Close()
	}()
	time.Sleep(6 * time.Second)
	l1.Close()
	wg.Wait()
	if err2 != nil {
		t.Error("unexpected error while acquiring 2nd lock:", err2)
	}
	if !locked {
		t.Fatal("concurrent lock flow is not working")
	}
}

func TestGet(t *testing.T) {
	t.Parallel()
	t.Run("happy path - data", func(t *testing.T) {
		db, _ := setupDB(t, pglock.WithCustomTable("TestGetHappyPathData"))
		defer db.Close()
		name := randStr()
		c, err := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
			pglock.WithCustomTable("TestGetHappyPathData"),
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
		db, _ := setupDB(t, pglock.WithCustomTable("TestGetHappyPathLock"))
		defer db.Close()
		name := randStr()
		const expectedOwner = "custom-owner"
		c, err := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
			pglock.WithOwner(expectedOwner),
			pglock.WithCustomTable("TestGetHappyPathLock"),
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
		db, _ := setupDB(t, pglock.WithCustomTable("TestGetUnknownKeyData"))
		defer db.Close()
		name := "lock-404"
		c, err := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
			pglock.WithCustomTable("TestGetUnknownKeyData"),
		)
		if err != nil {
			t.Fatal("cannot create lock client:", err)
		}
		if _, err := c.GetData(name); err == nil {
			t.Fatal("expected error not found on loading unknown key")
		} else if notFound := (&pglock.NotExistError{}); !errors.As(err, &notFound) {
			t.Fatal("unexpected error kind found on loading unknown key:", err)
		} else if !errors.Is(err, pglock.ErrLockNotFound) {
			t.Fatal("unexpected error found on loading unknown key:", err)
		}
	})
}

func TestLockData(t *testing.T) {
	t.Parallel()
	db, tableName := setupDB(t)
	defer db.Close()
	t.Run("reuse lock data", func(t *testing.T) {
		name := randStr()
		c, err := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
			pglock.WithCustomTable(tableName),
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
		name := randStr()
		c, err := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
			pglock.WithCustomTable(tableName),
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
	db := setupDBConn(t)
	defer db.Close()
	t.Run("happy path", func(t *testing.T) {
		tableName := randStr()
		defer func() {
			_, _ = db.Exec("DROP TABLE " + tableName)
		}()
		name := randStr()
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
		tableName := randStr()
		defer func() {
			_, _ = db.Exec("DROP TABLE " + tableName)
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

func TestCustomTableIdemPotent(t *testing.T) {
	t.Parallel()
	db := setupDBConn(t)
	defer db.Close()
	t.Run("happy path", func(t *testing.T) {
		tableName := randStr()
		defer func() {
			_, _ = db.Exec("DROP TABLE " + tableName)
		}()
		name := randStr()
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
		tableName := randStr()
		defer func() {
			_, _ = db.Exec("DROP TABLE " + tableName)
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
		if err := c.TryCreateTable(); err != nil {
			t.Fatal("unexpected error not found:", err)
		}
	})
}

func TestCanceledContext(t *testing.T) {
	db, tableName := setupDB(t)
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	name := randStr()
	c, err := pglock.New(
		db,
		pglock.WithLogger(&testLogger{t}),
		pglock.WithCustomTable(tableName),
	)
	if err != nil {
		t.Fatal("cannot create lock client:", err)
	}
	if _, err := c.AcquireContext(ctx, name); !errors.Is(err, pglock.ErrNotAcquired) {
		t.Fatal("canceled context should not be able to acquire locks")
	}
}

func TestDo(t *testing.T) {
	t.Parallel()
	db, tableName := setupDB(t)
	defer db.Close()
	t.Run("lost lock", func(t *testing.T) {
		const heartbeatFrequency = 1 * time.Second
		name := randStr()
		c, err := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
			pglock.WithLeaseDuration(5*time.Second),
			pglock.WithHeartbeatFrequency(heartbeatFrequency),
			pglock.WithCustomTable(tableName),
		)
		if err != nil {
			t.Fatal("cannot create lock client:", err)
		}
		ranOnce := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		lockErr := make(chan error, 1)
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
			if err != nil && !errors.Is(err, context.Canceled) {
				lockErr <- err
			}
		}()
		<-ranOnce
		select {
		case err := <-lockErr:
			t.Fatal("unexpected error while running under lock:", err)
		default:
		}
		t.Log("directly releasing lock")
		if err := releaseLockByName(db, tableName, name); err != nil {
			t.Fatalf("cannot forcefully release lock: %v", err)
		}
		wg.Wait()
	})

	t.Run("normally completed", func(t *testing.T) {
		const heartbeatFrequency = 1 * time.Second
		name := randStr()
		c, err := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
			pglock.WithLeaseDuration(5*time.Second),
			pglock.WithHeartbeatFrequency(heartbeatFrequency),
			pglock.WithCustomTable(tableName),
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
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal("unexpected error while running under lock:", err)
		}
	})

	t.Run("canceled context", func(t *testing.T) {
		const heartbeatFrequency = 1 * time.Second
		name := randStr()
		c, err := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
			pglock.WithLeaseDuration(5*time.Second),
			pglock.WithHeartbeatFrequency(heartbeatFrequency),
			pglock.WithCustomTable(tableName),
		)
		if err != nil {
			t.Fatal("cannot create lock client:", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err = c.Do(ctx, name, func(ctx context.Context, l *pglock.Lock) error {
			return nil
		})
		if !errors.Is(err, pglock.ErrNotAcquired) {
			t.Fatal("unexpected error while running under lock with canceled context:", err)
		}
	})

	t.Run("canceled heartbeat context", func(t *testing.T) {
		const heartbeatFrequency = 1 * time.Second
		name := randStr()
		c, err := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
			pglock.WithLeaseDuration(5*time.Second),
			pglock.WithHeartbeatFrequency(heartbeatFrequency),
			pglock.WithCustomTable(tableName),
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
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal("unexpected error while running under lock:", err)
		}
		t.Log("done")
	})

	t.Run("handle failIfLocked", func(t *testing.T) {
		name := randStr()
		c, err := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
			pglock.WithLeaseDuration(5*time.Second),
			pglock.WithHeartbeatFrequency(0),
			pglock.WithCustomTable(tableName),
		)
		if err != nil {
			t.Fatal("cannot create lock client:", err)
		}
		if _, err := c.Acquire(name); err != nil {
			t.Fatal("cannot grab lock:", err)
		}
		err = c.Do(context.Background(), name, func(ctx context.Context, l *pglock.Lock) error {
			return errors.New("should not have been executed")
		}, pglock.FailIfLocked())
		if !errors.Is(err, pglock.ErrNotAcquired) {
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
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal("unexpected error while running under lock:", err)
		}
		t.Log("done")
	})
}

func TestOwner(t *testing.T) {
	t.Parallel()
	db, tableName := setupDB(t)
	defer db.Close()
	const expectedOwner = "custom-owner"
	lockName := randStr()
	c, err := pglock.New(
		db,
		pglock.WithLogger(&testLogger{t}),
		pglock.WithLeaseDuration(5*time.Second),
		pglock.WithHeartbeatFrequency(1*time.Second),
		pglock.WithOwner(expectedOwner),
		pglock.WithCustomTable(tableName),
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
	row := db.QueryRow("SELECT owner FROM "+tableName+" WHERE name = $1", lockName)
	var foundOwner string
	if err := row.Scan(&foundOwner); err != nil {
		t.Fatalf("cannot load owner from the database: %v", err)
	}
	if foundOwner != expectedOwner {
		t.Fatalf("owner not stored in the lock table: %s", foundOwner)
	}
}

func releaseLockByName(db *sql.DB, tblName, name string) error {
	const serializationErrorCode = "40001"
	for {
		_, err := db.Exec("UPDATE "+tblName+" SET record_version_number = NULL WHERE name = $1", name)
		if errPQ := (&pq.Error{}); errors.As(err, &errPQ) {
			if errPQ.Code == serializationErrorCode {
				continue
			}
		}
		if err != nil {
			return fmt.Errorf("cannot release lock by name: %w", err)
		}
		return nil
	}
}

func TestSendHeartbeat(t *testing.T) {
	db, tableName := setupDB(t)
	defer db.Close()
	t.Run("bad sendHeartbeat", func(t *testing.T) {
		c, err := pglock.New(
			db,
			pglock.WithLogger(&testLogger{t}),
			pglock.WithLeaseDuration(5*time.Second),
			pglock.WithHeartbeatFrequency(0),
			pglock.WithCustomTable(tableName),
		)
		if err != nil {
			t.Fatal("cannot create lock client:", err)
		}
		name := randStr()
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
			pglock.WithCustomTable(tableName),
		)
		if err != nil {
			t.Fatal("cannot create lock client:", err)
		}
		name := randStr()
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
	db, tableName := setupDB(t)
	defer db.Close()
	c, err := pglock.New(
		db,
		pglock.WithLogger(&testLogger{t}),
		pglock.WithLeaseDuration(5*time.Second),
		pglock.WithHeartbeatFrequency(1),
		pglock.WithCustomTable(tableName),
	)
	if err != nil {
		t.Fatal("cannot create lock client:", err)
	}
	name := randStr()
	l, err := c.Acquire(name)
	if err != nil {
		t.Fatal("unexpected error while acquiring lock:", err)
	}

	var wg sync.WaitGroup
	done := make(chan struct{})
	wg.Add(1)
	releaseErr := make(chan error, 1)
	go func() {
		defer wg.Done()
		if err := c.Release(l); err != nil {
			releaseErr <- err
			return
		}
		close(done)
	}()
	select {
	case err := <-releaseErr:
		t.Fatal("unexpected error while releasing lock:", err)
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock between sendHeartbeat and release")
	case <-done:
	}
	wg.Wait()
}

func TestReleaseLostLock(t *testing.T) {
	db, tableName := setupDB(t)
	defer db.Close()
	name := randStr()
	c, err := pglock.New(
		db,
		pglock.WithLogger(&testLogger{t}),
		pglock.WithLeaseDuration(5*time.Second),
		pglock.WithHeartbeatFrequency(0),
		pglock.WithCustomTable(tableName),
	)
	if err != nil {
		t.Fatal("cannot create lock client:", err)
	}
	l, err := c.Acquire(name)
	if err != nil {
		t.Fatal("cannot acquire lock:", err)
	}
	t.Log("directly releasing lock")
	if err := releaseLockByName(db, tableName, name); err != nil {
		t.Fatalf("cannot forcefully release lock: %v", err)
	}
	t.Log(c.Release(l))
	if !l.IsReleased() {
		t.Fatal("expected lock to be released")
	}
}

func TestIssue29(t *testing.T) {
	testfunc := func(t *testing.T, db *sql.DB, tableName string) {
		t.Helper()
		lockName := randStr()
		c, err := pglock.UnsafeNew(
			db,
			pglock.WithLeaseDuration(2*time.Second),
			pglock.WithHeartbeatFrequency(0),
			pglock.WithCustomTable(tableName),
		)
		if err != nil {
			t.Fatal("cannot create lock client:", err)
		}
		var (
			foundErrMu sync.Mutex
			foundErr   error
			wg         sync.WaitGroup
		)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for i := 0; i < 1024; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					default:
					}
					lock, err := c.Acquire(lockName)
					if err != nil {
						if strings.Contains(err.Error(), "could not serialize access due to") {
							foundErrMu.Lock()
							foundErr = err
							foundErrMu.Unlock()
						}
						return
					}
					lock.Close()
				}
			}()
		}
		wg.Wait()
		if foundErr != nil {
			t.Error("serialization error found", foundErr)
		}
	}
	t.Run("lib/pq", func(t *testing.T) {
		db, tableName := setupDB(t)
		defer db.Close()
		testfunc(t, db, tableName)
	})
	t.Run("jackc/pgx", func(t *testing.T) {
		db, tableName := setupCustomDB(t, "pgx")
		defer db.Close()
		testfunc(t, db, tableName)
	})
}

func parallelAcquire(tb testing.TB, maxConcurrency int) {
	tb.Helper()
	db, tableName := setupDB(tb)
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, maxConcurrency)
	for i := 0; i < maxConcurrency; i++ {
		go func() {
			c, err := pglock.New(
				db,
				pglock.WithLogger(&discardLogging{}),
				pglock.WithLeaseDuration(5*time.Second),
				pglock.WithHeartbeatFrequency(1),
				pglock.WithCustomTable(tableName),
			)
			if err != nil {
				err := fmt.Errorf("cannot start lock client: %w", err)
				select {
				case errCh <- err:
				default:
				}
				return
			}
			name := randStr()
			for {
				l, err := c.AcquireContext(ctx, name)
				if err != nil {
					err := fmt.Errorf("cannot grab the lock (%q): %w", name, err)
					select {
					case errCh <- err:
					default:
					}
					return
				}
				select {
				case <-time.After(time.Duration(rand.Intn(30000)) * time.Millisecond):
					err = c.ReleaseContext(ctx, l)
					if err != nil {
						err := fmt.Errorf("cannot grab the lock (%q): %w", name, err)
						select {
						case errCh <- err:
						default:
						}
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	tb.Log("all clients triggered")
	select {
	case <-time.After(10 * time.Second):
		cancel()
	case <-ctx.Done():
		return
	case err := <-errCh:
		// If the context is cancelled its likely we will get a lot of
		// errors of in flight operations. We don't care about those so
		// we will not Fail on any error that occurred after context
		// cancellation
		if ctx.Err() != nil {
			return
		}
		cancel()
		tb.Error("trapped error:", err)
		tb.Log("draining errors:")
		for err := range errCh {
			tb.Error("drained error:", err)
		}
	}
}

func TestParallelAcquire(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	parallelAcquire(t, 100)
}

func TestGetAllLocks(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("postgres", *dsn)
	if err != nil {
		t.Fatal("cannot connect to test database server:", err)
	}
	const expectedOwner = "owner"
	c, err := pglock.New(
		db,
		pglock.WithCustomTable("getAllLocks"),
		pglock.WithHeartbeatFrequency(0),
		pglock.WithLogger(&discardLogging{}),
		pglock.WithOwner(expectedOwner),
	)
	if err != nil {
		t.Fatal("cannot connect:", err)
	}
	defer db.Close()
	_ = c.CreateTable()
	defer func() { _ = c.DropTable() }()
	names := make(map[string]struct{})
	expected := []byte("42")
	for i := 0; i < 5; i++ {
		name := randStr()
		if _, err := c.Acquire(name, pglock.WithData(expected)); err != nil {
			t.Fatal("unexpected error while acquiring lock:", err)
		}
		names[name] = struct{}{}
	}
	t.Logf("%#v", names)
	locks, err := c.GetAllLocks()
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range locks {
		t.Log(l.Name(), l.Owner(), string(l.Data()))
		if l.Owner() != expectedOwner {
			t.Error("mismatched owner:", l.Owner())
		}
		if _, ok := names[l.Name()]; !ok {
			t.Error("unknown lock name:", l.Name())
		}
		delete(names, l.Name())
		if !bytes.Equal(expected, l.Data()) {
			t.Error("missing expected data")
		}
		if t.Failed() {
			break
		}
	}
}

func TestStaleAfterRelease(t *testing.T) {
	db, tableName := setupDB(t)
	defer db.Close()
	db.SetMaxOpenConns(30)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)

	c, err := pglock.New(db, pglock.WithOwner("TestStaleAfterRelease"), pglock.WithCustomTable(tableName))
	if err != nil {
		t.Fatal("cannot connect:", err)
	}
	if _, err := db.Exec("DELETE FROM " + tableName + " WHERE owner = 'TestStaleAfterRelease'"); err != nil {
		t.Fatal("cannot reset table:", err)
	}
	var (
		group errgroup.Group
		start = make(chan struct{})
	)
	for i := 0; i < 100; i++ {
		lockName := fmt.Sprint("lock-name-", i)
		group.Go(func() error {
			<-start
			l, err := c.Acquire(lockName)
			if err != nil {
				return fmt.Errorf("cannot acquire lock (%q): %w", lockName, err)
			}
			t.Log(lockName, "acquired")
			time.Sleep(6 * time.Second)
			if err := l.Close(); err != nil {
				return fmt.Errorf("cannot release lock (%q): %w", lockName, err)
			}
			t.Log(lockName, "released")
			return nil
		})
	}
	close(start)
	errGroup := group.Wait()
	if errGroup != nil {
		t.Fatal("unexpected error: ", errGroup)
	}
}

func TestOverflowSequence(t *testing.T) {
	t.Parallel()
	db := setupDBConn(t)
	defer db.Close()
	tableName := randStr()
	defer func() {
		_, _ = db.Exec("DROP TABLE " + tableName)
	}()
	name := randStr()
	c, err := pglock.New(
		db,
		pglock.WithLevelLogger(&testLevelLogger{t}),
		pglock.WithCustomTable(tableName),
	)
	if err != nil {
		t.Fatal("cannot create lock client:", err)
	}
	if err := c.CreateTable(); err != nil {
		t.Fatal("cannot create table:", err)
	}
	const maxBigInt = 9223372036854775807
	alterSequence := fmt.Sprint("ALTER SEQUENCE ", tableName, "_rvn RESTART WITH ", maxBigInt)
	t.Log(alterSequence)
	if _, err := db.Exec(alterSequence); err != nil {
		t.Fatal("cannot reset sequence:", err)
	}
	for i := 0; i < 10; i++ {
		l1, err := c.Acquire(name)
		if err != nil {
			t.Fatal("unexpected error while acquiring lock:", err)
		}
		l1.Close()
	}
}
