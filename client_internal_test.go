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

package pglock

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/lib/pq"
)

func TestTypedError(t *testing.T) {
	if err, e := typedError(fmt.Errorf("random error"), ""), (&OtherError{}); !errors.As(err, &e) {
		t.Errorf("mistyped error found (OtherError): %#v", err)
	}

	if err, e := typedError(sql.ErrNoRows, ""), (&NotExistError{}); !errors.As(err, &e) {
		t.Errorf("mistyped error found (NotExistError): %#v", err)
	}

	if err, e := typedError(&net.OpError{}, ""), (&UnavailableError{}); !errors.As(err, &e) {
		t.Errorf("mistyped error found (UnavailableError): %#v", err)
	}

	if err, e := typedError(&pq.Error{}, ""), (&pq.Error{}); !errors.As(err, &e) {
		t.Errorf("mistyped error found (pq.Error): %#v", err)
	}

	if err, e := typedError(&pq.Error{Code: "40001"}, ""), (&FailedPreconditionError{}); !errors.As(err, &e) {
		t.Errorf("mistyped error found (FailedPreconditionError): %#v", err)
	}
}

func TestRetry(t *testing.T) {
	t.Run("type check", func(t *testing.T) {
		c := &Client{
			log: &flatLogger{&testLogger{t}},
		}
		errs := []error{
			&FailedPreconditionError{errors.New("failed precondition")},
			&OtherError{errors.New("other error")},
		}
		err := c.retry(func() error {
			var err error
			err, errs = errs[0], errs[1:]
			return err
		})
		if otherErr := (&OtherError{}); !errors.As(err, &otherErr) {
			t.Fatal("unexpected error kind found")
		}
	})
	t.Run("max retries", func(t *testing.T) {
		c := &Client{
			log: &flatLogger{log.New(io.Discard, "", 0)},
		}
		var retries int
		err := c.retry(func() error {
			retries++
			return &FailedPreconditionError{errors.New("failed precondition")}
		})
		if failedPreconditionErr := (&FailedPreconditionError{}); !errors.As(err, &failedPreconditionErr) {
			t.Fatal("unexpected error kind found")
		}
		if retries != maxRetries {
			t.Fatal("unexpected retries count found")
		}
		t.Log(retries, maxRetries)
	})
}

type testLogger struct {
	t *testing.T
}

func (t *testLogger) Println(v ...interface{}) {
	t.t.Log(v...)
}

func TestDBErrorHandling(t *testing.T) {
	setup := func() (*Client, sqlmock.Sqlmock, *Lock) {
		db, err := sql.Open("postgres", "")
		if err != nil {
			t.Fatal("cannot connect to test database server:", err)
		}
		client, _ := New(db, WithHeartbeatFrequency(0))
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal("cannot create mock:", err)
		}
		client.db = db
		ctx, cancel := context.WithCancel(context.Background())
		return client, mock, &Lock{
			heartbeatContext: ctx,
			heartbeatCancel:  cancel,
			leaseDuration:    time.Minute,
		}
	}
	t.Run("acquire", func(t *testing.T) {
		t.Run("bad tx", func(t *testing.T) {
			client, mock, _ := setup()
			badTx := errors.New("transaction begin error")
			mock.ExpectQuery(`SELECT nextval\('locks_rvn'\)`).WillReturnRows(sqlmock.NewRows([]string{"nextval"}).AddRow(1))
			mock.ExpectBegin().WillReturnError(badTx)
			if _, err := client.Acquire("bad-tx"); !errors.Is(err, badTx) {
				t.Errorf("expected tx error missing: %v", err)
			}
		})
		t.Run("bad rvn", func(t *testing.T) {
			client, mock, _ := setup()
			badRVN := errors.New("cannot load next RVN")
			mock.ExpectQuery(`SELECT nextval\('locks_rvn'\)`).WillReturnError(badRVN)
			if _, err := client.Acquire("bad-rvn"); !errors.Is(err, badRVN) {
				t.Errorf("expected RVN error missing: %v", err)
			}
		})
		t.Run("bad insert", func(t *testing.T) {
			client, mock, _ := setup()
			badInsert := errors.New("cannot insert")
			mock.ExpectQuery(`SELECT nextval\('locks_rvn'\)`).WillReturnRows(sqlmock.NewRows([]string{"nextval"}).AddRow(1))
			mock.ExpectBegin()
			mock.ExpectExec(`INSERT INTO locks (.+)`).WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnError(badInsert)
			if _, err := client.Acquire("bad-insert"); !errors.Is(err, badInsert) {
				t.Errorf("expected RVN error missing: %v", err)
			}
		})
		t.Run("bad RVN confirmation", func(t *testing.T) {
			client, mock, _ := setup()
			badRVN := errors.New("cannot confirm RVN")
			mock.ExpectQuery(`SELECT nextval\('locks_rvn'\)`).WillReturnRows(sqlmock.NewRows([]string{"nextval"}).AddRow(1))
			mock.ExpectBegin()
			mock.ExpectExec(`INSERT INTO locks (.+)`).WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery(`SELECT "record_version_number", "data", "owner" FROM locks WHERE name = (.+)`).WithArgs(sqlmock.AnyArg()).WillReturnError(badRVN)
			if _, err := client.Acquire("bad-insert"); !errors.Is(err, badRVN) {
				t.Errorf("expected RVN confirmation error missing: %v", err)
			}
		})
		t.Run("bad commit", func(t *testing.T) {
			client, mock, _ := setup()
			badCommit := errors.New("cannot confirm RVN")
			mock.ExpectQuery(`SELECT nextval\('locks_rvn'\)`).WillReturnRows(sqlmock.NewRows([]string{"nextval"}).AddRow(1))
			mock.ExpectBegin()
			mock.ExpectExec(`INSERT INTO locks (.+)`).WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery(`SELECT "record_version_number", "data", "owner" FROM locks WHERE name = (.+)`).
				WithArgs(sqlmock.AnyArg()).
				WillReturnRows(
					sqlmock.NewRows([]string{
						"record_version_number",
						"data",
						"owner",
					}).AddRow(1, []byte{}, "owner"),
				)
			mock.ExpectCommit().WillReturnError(badCommit)
			if _, err := client.Acquire("bad-insert"); !errors.Is(err, badCommit) {
				t.Errorf("expected commit error missing: %v", err)
			}
		})
	})
	t.Run("release", func(t *testing.T) {
		t.Run("bad tx", func(t *testing.T) {
			client, mock, fakeLock := setup()
			badTx := errors.New("transaction begin error")
			mock.ExpectBegin().WillReturnError(badTx)
			if err := client.Release(fakeLock); !errors.Is(err, badTx) {
				t.Errorf("expected tx error missing: %v", err)
			}
		})
		t.Run("bad update", func(t *testing.T) {
			client, mock, fakeLock := setup()
			badUpdate := errors.New("cannot update")
			mock.ExpectBegin()
			mock.ExpectExec(`UPDATE locks (.+)`).WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnError(badUpdate)
			if err := client.Release(fakeLock); !errors.Is(err, badUpdate) {
				t.Errorf("expected update error missing: %v", err)
			}
		})
		t.Run("bad update result", func(t *testing.T) {
			client, mock, fakeLock := setup()
			badUpdateResult := errors.New("cannot grab update result")
			mock.ExpectBegin()
			mock.ExpectExec(`UPDATE locks (.+)`).WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnResult(sqlmock.NewErrorResult(badUpdateResult))
			if err := client.Release(fakeLock); !errors.Is(err, badUpdateResult) {
				t.Errorf("expected update result error missing: %v", err)
			}
		})
		t.Run("bad delete", func(t *testing.T) {
			client, mock, fakeLock := setup()
			badDelete := errors.New("cannot delete lock entry")
			mock.ExpectBegin()
			mock.ExpectExec(`UPDATE locks (.+)`).WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec(`DELETE FROM locks (.+)`).WithArgs(sqlmock.AnyArg()).WillReturnError(badDelete)
			if err := client.Release(fakeLock); !errors.Is(err, badDelete) {
				t.Errorf("expected delete error missing: %v", err)
			}
		})
		t.Run("bad commit", func(t *testing.T) {
			client, mock, fakeLock := setup()
			badCommit := errors.New("cannot commit release")
			mock.ExpectBegin()
			mock.ExpectExec(`UPDATE locks (.+)`).WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec(`DELETE FROM locks (.+)`).WithArgs(sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectCommit().WillReturnError(badCommit)
			if err := client.Release(fakeLock); !errors.Is(err, badCommit) {
				t.Errorf("expected commit error missing: %v", err)
			}
		})
	})
	t.Run("heartbeat", func(t *testing.T) {
		t.Run("bad rvn", func(t *testing.T) {
			client, mock, fakeLock := setup()
			badRVN := errors.New("cannot load next RVN")
			mock.ExpectQuery(`SELECT nextval\('locks_rvn'\)`).WillReturnError(badRVN)
			if err := client.SendHeartbeat(context.Background(), fakeLock); !errors.Is(err, badRVN) {
				t.Errorf("expected RVN error missing: %v", err)
			}
		})
		t.Run("bad update", func(t *testing.T) {
			client, mock, fakeLock := setup()
			badUpdate := errors.New("cannot insert")
			mock.ExpectQuery(`SELECT nextval\('locks_rvn'\)`).WillReturnRows(sqlmock.NewRows([]string{"nextval"}).AddRow(1))
			mock.ExpectExec(`UPDATE locks (.+)`).WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnError(badUpdate)
			if err := client.SendHeartbeat(context.Background(), fakeLock); !errors.Is(err, badUpdate) {
				t.Errorf("expected RVN error missing: %v", err)
			}
		})
		t.Run("bad RVN confirmation", func(t *testing.T) {
			client, mock, fakeLock := setup()
			badRVN := errors.New("cannot confirm RVN")
			mock.ExpectQuery(`SELECT nextval\('locks_rvn'\)`).WillReturnRows(sqlmock.NewRows([]string{"nextval"}).AddRow(1))
			mock.ExpectExec(`UPDATE locks (.+)`).WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnResult(sqlmock.NewErrorResult(badRVN))
			if err := client.SendHeartbeat(context.Background(), fakeLock); !errors.Is(err, badRVN) {
				t.Errorf("expected RVN confirmation error missing: %v", err)
			}
		})
	})

	t.Run("GetAllLocks", func(t *testing.T) {
		t.Run("bad query", func(t *testing.T) {
			client, mock, _ := setup()
			badQuery := errors.New("cannot run query")
			mock.ExpectQuery(`SELECT "name", "owner", "data"`).WillReturnError(badQuery)
			_, err := client.GetAllLocksContext(context.Background())
			if !errors.Is(err, badQuery) {
				t.Errorf("expected error missing: %v", err)
			}
		})
		t.Run("bad row scan", func(t *testing.T) {
			client, mock, _ := setup()
			rows := sqlmock.NewRows([]string{"name"}).
				AddRow(1)
			mock.ExpectQuery(`SELECT "name", "owner", "data"`).WillReturnRows(rows)
			_, err := client.GetAllLocksContext(context.Background())
			if err == nil {
				t.Errorf("expected error missing: %v", err)
			}
		})
		t.Run("bad scan", func(t *testing.T) {
			client, mock, _ := setup()
			badScan := errors.New("bad Scan")
			rows := sqlmock.NewRows([]string{"name", "owner", "data"}).
				AddRow("name", "owner", "data").
				RowError(0, badScan)
			mock.ExpectQuery(`SELECT "name", "owner", "data"`).WillReturnRows(rows)
			_, err := client.GetAllLocksContext(context.Background())
			if !errors.Is(err, badScan) {
				t.Errorf("expected error missing: %v", err)
			}
		})
	})
}

func Test_waitFor(t *testing.T) {
	const expectedWait = 10 * time.Second
	t.Run("wait", func(t *testing.T) {
		start := time.Now()
		waitFor(context.Background(), expectedWait)
		if time.Since(start) < expectedWait {
			t.Fatal("did not wait")
		}
	})
	t.Run("cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		start := time.Now()
		waitFor(ctx, expectedWait)
		if time.Since(start) >= expectedWait {
			t.Fatal("did not cancel")
		}
	})
}

func Test_heartbeatLogging(t *testing.T) {
	t.Run("cancelled", func(t *testing.T) {
		db, err := sql.Open("postgres", "")
		if err != nil {
			t.Fatal("cannot connect to test database server:", err)
		}
		logger := &testBufferLogger{}
		client, _ := New(db, WithHeartbeatFrequency(0), WithLogger(logger))
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal("cannot create mock:", err)
		}
		client.db = db
		ctx, cancel := context.WithCancel(context.Background())
		fakeLock := &Lock{
			heartbeatContext: ctx,
			heartbeatCancel:  cancel,
			leaseDuration:    time.Minute,
		}
		hbCtx, hbCancel := context.WithCancel(context.Background())
		hbCancel()
		mock.ExpectQuery(`SELECT nextval\('locks_rvn'\)`).WillReturnError(hbCtx.Err())
		fakeLock.heartbeatWG.Add(1)
		client.heartbeat(hbCtx, fakeLock)
		t.Log(logger.buf.String())
		if strings.Contains(logger.buf.String(), context.Canceled.Error()) {
			t.Fatal("must not log context cancellations")
		}
	})
	t.Run("errored", func(t *testing.T) {
		db, err := sql.Open("postgres", "")
		if err != nil {
			t.Fatal("cannot connect to test database server:", err)
		}
		logger := &testBufferLogger{}
		client, _ := New(db, WithHeartbeatFrequency(0), WithLogger(logger))
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal("cannot create mock:", err)
		}
		client.db = db
		ctx, cancel := context.WithCancel(context.Background())
		fakeLock := &Lock{
			heartbeatContext: ctx,
			heartbeatCancel:  cancel,
			leaseDuration:    time.Minute,
		}
		errExpected := errors.New("expected error")
		mock.ExpectQuery(`SELECT nextval\('locks_rvn'\)`).WillReturnError(errExpected)
		fakeLock.heartbeatWG.Add(1)
		client.heartbeat(context.Background(), fakeLock)
		t.Log(logger.buf.String())
		if !strings.Contains(logger.buf.String(), errExpected.Error()) {
			t.Fatal("expected error missing")
		}
	})
}

type testBufferLogger struct {
	buf bytes.Buffer
}

func (t *testBufferLogger) Println(v ...interface{}) {
	fmt.Fprintln(&t.buf, v...)
}
