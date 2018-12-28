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
	"net"
	"testing"

	"cirello.io/errors"
	"github.com/lib/pq"
)

func TestTypedError(t *testing.T) {
	errs := []struct {
		err   error
		typed bool
	}{
		{fmt.Errorf("random error"), false},
		{sql.ErrNoRows, true},
		{&net.OpError{}, true},
		{&pq.Error{}, true},
	}
	for _, err := range errs {
		if err.err == nil {
			continue
		}
		typeErr := typedError(err.err)
		e, ok := typeErr.(*errors.Error)
		if !ok {
			continue
		}
		if err.typed && e.Kind == errors.Other {
			t.Errorf("untyped error found: %#v", e)
		} else if !err.typed && e.Kind != errors.Other {
			t.Errorf("mistyped error found: %#v", e)
		}
	}
}
