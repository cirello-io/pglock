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
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"testing"

	"github.com/lib/pq"
	"golang.org/x/xerrors"
)

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
