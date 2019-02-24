/*
Copyright 2019 github.com/ucirello

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
	"testing"

	"golang.org/x/xerrors"
)

func TestErrorKind(t *testing.T) {
	baseErr := xerrors.New("base error")

	notExistsErr := &NotExistError{baseErr}
	if !xerrors.Is(notExistsErr, baseErr) {
		t.Error("cannot unwrap error for NotExistError")
	}
	t.Logf("notExistsErr is %q", notExistsErr)

	unavailableErr := &UnavailableError{baseErr}
	if !xerrors.Is(unavailableErr, baseErr) {
		t.Error("cannot unwrap error for UnavailableError")
	}
	t.Logf("unavailableErr is %q", unavailableErr)

	failedPreconditionErr := &FailedPreconditionError{baseErr}
	if !xerrors.Is(failedPreconditionErr, baseErr) {
		t.Error("cannot unwrap error for FailedPreconditionError")
	}
	t.Logf("failedPreconditionErr is %q", failedPreconditionErr)

	otherErr := &OtherError{baseErr}
	if !xerrors.Is(otherErr, baseErr) {
		t.Error("cannot unwrap error for OtherError")
	}
	t.Logf("otherErr is %q", otherErr)
}
