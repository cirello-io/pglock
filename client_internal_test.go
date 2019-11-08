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
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/lib/pq"
	"golang.org/x/xerrors"
)

func (c *Client) WaitHeartbeats() {
	c.heartbeatWG.Wait()
}

func TestTypedError(t *testing.T) {
	if err, e := typedError(fmt.Errorf("random error"), ""), (&OtherError{}); !xerrors.As(err, &e) {
		t.Errorf("mistyped error found (OtherError): %#v", err)
	}

	if err, e := typedError(sql.ErrNoRows, ""), (&NotExistError{}); !xerrors.As(err, &e) {
		t.Errorf("mistyped error found (NotExistError): %#v", err)
	}

	if err, e := typedError(&net.OpError{}, ""), (&UnavailableError{}); !xerrors.As(err, &e) {
		t.Errorf("mistyped error found (UnavailableError): %#v", err)
	}

	if err, e := typedError(&pq.Error{}, ""), (&pq.Error{}); !xerrors.As(err, &e) {
		t.Errorf("mistyped error found (pq.Error): %#v", err)
	}

	if err, e := typedError(&pq.Error{Code: "40001"}, ""), (&FailedPreconditionError{}); !xerrors.As(err, &e) {
		t.Errorf("mistyped error found (FailedPreconditionError): %#v", err)
	}
}

func TestRetry(t *testing.T) {
	t.Run("type check", func(t *testing.T) {
		c := &Client{
			log: &testLogger{t},
		}
		errs := []error{
			&FailedPreconditionError{xerrors.New("failed precondition")},
			&OtherError{xerrors.New("other error")},
		}
		err := c.retry(func() error {
			var err error
			err, errs = errs[0], errs[1:]
			return err
		})
		if otherErr := (&OtherError{}); !xerrors.As(err, &otherErr) {
			t.Fatal("unexpected error kind found")
		}
	})
	t.Run("max retries", func(t *testing.T) {
		c := &Client{
			log: log.New(ioutil.Discard, "", 0),
		}
		var retries int
		err := c.retry(func() error {
			retries++
			return &FailedPreconditionError{xerrors.New("failed precondition")}
		})
		if failedPreconditionErr := (&FailedPreconditionError{}); !xerrors.As(err, &failedPreconditionErr) {
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
	setup := func() (*Client, sqlmock.Sqlmock) {
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
		return client, mock
	}
	t.Run("acquire", func(t *testing.T) {
		t.Run("bad tx", func(t *testing.T) {
			client, mock := setup()
			badTx := xerrors.New("transaction begin error")
			mock.ExpectBegin().WillReturnError(badTx)
			if _, err := client.Acquire("bad-tx"); !xerrors.Is(err, badTx) {
				t.Errorf("expected tx error missing: %v", err)
			}
		})
		t.Run("bad rvn", func(t *testing.T) {
			client, mock := setup()
			badRVN := xerrors.New("cannot load next RVN")
			mock.ExpectBegin()
			mock.ExpectQuery(`SELECT nextval\('locks_rvn'\)`).WillReturnError(badRVN)
			if _, err := client.Acquire("bad-rvn"); !xerrors.Is(err, badRVN) {
				t.Errorf("expected RVN error missing: %v", err)
			}
		})
		t.Run("bad insert", func(t *testing.T) {
			client, mock := setup()
			badInsert := xerrors.New("cannot insert")
			mock.ExpectBegin()
			mock.ExpectQuery(`SELECT nextval\('locks_rvn'\)`).WillReturnRows(sqlmock.NewRows([]string{"nextval"}).AddRow(1))
			mock.ExpectExec(`INSERT INTO locks (.+)`).WillReturnError(badInsert)
			if _, err := client.Acquire("bad-insert"); !xerrors.Is(err, badInsert) {
				t.Errorf("expected RVN error missing: %v", err)
			}
		})
		t.Run("bad RVN confirmation", func(t *testing.T) {
			client, mock := setup()
			badRVN := xerrors.New("cannot confirm RVN")
			mock.ExpectBegin()
			mock.ExpectQuery(`SELECT nextval\('locks_rvn'\)`).WillReturnRows(sqlmock.NewRows([]string{"nextval"}).AddRow(1))
			mock.ExpectExec(`INSERT INTO locks (.+)`).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery(`SELECT "record_version_number", "data", "owner" FROM locks WHERE name = (.+)`).WillReturnError(badRVN)
			if _, err := client.Acquire("bad-insert"); !xerrors.Is(err, badRVN) {
				t.Errorf("expected RVN confirmation error missing: %v", err)
			}
		})
		t.Run("bad commit", func(t *testing.T) {
			client, mock := setup()
			badCommit := xerrors.New("cannot confirm RVN")
			mock.ExpectBegin()
			mock.ExpectQuery(`SELECT nextval\('locks_rvn'\)`).WillReturnRows(sqlmock.NewRows([]string{"nextval"}).AddRow(1))
			mock.ExpectExec(`INSERT INTO locks (.+)`).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery(`SELECT "record_version_number", "data", "owner" FROM locks WHERE name = (.+)`).WillReturnRows(
				sqlmock.NewRows([]string{
					"record_version_number",
					"data",
					"owner",
				}).AddRow(1, []byte{}, "owner"),
			)
			mock.ExpectCommit().WillReturnError(badCommit)
			if _, err := client.Acquire("bad-insert"); !xerrors.Is(err, badCommit) {
				t.Errorf("expected commit error missing: %v", err)
			}
		})
	})
	fakeLock := &Lock{
		leaseDuration: time.Minute,
	}
	t.Run("release", func(t *testing.T) {
		t.Run("bad tx", func(t *testing.T) {
			client, mock := setup()
			badTx := xerrors.New("transaction begin error")
			mock.ExpectBegin().WillReturnError(badTx)
			if err := client.Release(fakeLock); !xerrors.Is(err, badTx) {
				t.Errorf("expected tx error missing: %v", err)
			}
		})
		t.Run("bad update", func(t *testing.T) {
			client, mock := setup()
			badUpdate := xerrors.New("cannot update")
			mock.ExpectBegin()
			mock.ExpectExec(`UPDATE locks (.+)`).WillReturnError(badUpdate)
			if err := client.Release(fakeLock); !xerrors.Is(err, badUpdate) {
				t.Errorf("expected update error missing: %v", err)
			}
		})
		t.Run("bad update result", func(t *testing.T) {
			client, mock := setup()
			badUpdateResult := xerrors.New("cannot grab update result")
			mock.ExpectBegin()
			mock.ExpectExec(`UPDATE locks (.+)`).WillReturnResult(sqlmock.NewErrorResult(badUpdateResult))
			if err := client.Release(fakeLock); !xerrors.Is(err, badUpdateResult) {
				t.Errorf("expected update result error missing: %v", err)
			}
		})
		t.Run("bad delete", func(t *testing.T) {
			client, mock := setup()
			badDelete := xerrors.New("cannot delete lock entry")
			mock.ExpectBegin()
			mock.ExpectExec(`UPDATE locks (.+)`).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec(`DELETE FROM locks (.+)`).WillReturnError(badDelete)
			if err := client.Release(fakeLock); !xerrors.Is(err, badDelete) {
				t.Errorf("expected delete error missing: %v", err)
			}
		})
		t.Run("bad commit", func(t *testing.T) {
			client, mock := setup()
			badCommit := xerrors.New("cannot commit release")
			mock.ExpectBegin()
			mock.ExpectExec(`UPDATE locks (.+)`).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec(`DELETE FROM locks (.+)`).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectCommit().WillReturnError(badCommit)
			if err := client.Release(fakeLock); !xerrors.Is(err, badCommit) {
				t.Errorf("expected commit error missing: %v", err)
			}
		})
	})
	t.Run("heartbeat", func(t *testing.T) {
		t.Run("bad tx", func(t *testing.T) {
			client, mock := setup()
			badTx := xerrors.New("transaction begin error")
			mock.ExpectBegin().WillReturnError(badTx)
			if err := client.SendHeartbeat(context.Background(), fakeLock); !xerrors.Is(err, badTx) {
				t.Errorf("expected tx error missing: %v", err)
			}
		})
		t.Run("bad rvn", func(t *testing.T) {
			client, mock := setup()
			badRVN := xerrors.New("cannot load next RVN")
			mock.ExpectBegin()
			mock.ExpectQuery(`SELECT nextval\('locks_rvn'\)`).WillReturnError(badRVN)
			if err := client.SendHeartbeat(context.Background(), fakeLock); !xerrors.Is(err, badRVN) {
				t.Errorf("expected RVN error missing: %v", err)
			}
		})
		t.Run("bad insert", func(t *testing.T) {
			client, mock := setup()
			badUpdate := xerrors.New("cannot insert")
			mock.ExpectBegin()
			mock.ExpectQuery(`SELECT nextval\('locks_rvn'\)`).WillReturnRows(sqlmock.NewRows([]string{"nextval"}).AddRow(1))
			mock.ExpectExec(`UPDATE locks (.+)`).WillReturnError(badUpdate)
			if err := client.SendHeartbeat(context.Background(), fakeLock); !xerrors.Is(err, badUpdate) {
				t.Errorf("expected RVN error missing: %v", err)
			}
		})
		t.Run("bad RVN confirmation", func(t *testing.T) {
			client, mock := setup()
			badRVN := xerrors.New("cannot confirm RVN")
			mock.ExpectBegin()
			mock.ExpectQuery(`SELECT nextval\('locks_rvn'\)`).WillReturnRows(sqlmock.NewRows([]string{"nextval"}).AddRow(1))
			mock.ExpectExec(`UPDATE locks (.+)`).WillReturnResult(sqlmock.NewErrorResult(badRVN))
			if err := client.SendHeartbeat(context.Background(), fakeLock); !xerrors.Is(err, badRVN) {
				t.Errorf("expected RVN confirmation error missing: %v", err)
			}
		})
		t.Run("bad commit", func(t *testing.T) {
			client, mock := setup()
			badCommit := xerrors.New("cannot confirm RVN")
			mock.ExpectBegin()
			mock.ExpectQuery(`SELECT nextval\('locks_rvn'\)`).WillReturnRows(sqlmock.NewRows([]string{"nextval"}).AddRow(1))
			mock.ExpectExec(`UPDATE locks (.+)`).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectCommit().WillReturnError(badCommit)
			if err := client.SendHeartbeat(context.Background(), fakeLock); !xerrors.Is(err, badCommit) {
				t.Errorf("expected commit error missing: %v", err)
			}
		})
	})
}
