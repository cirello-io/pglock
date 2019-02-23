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
	"fmt"

	"golang.org/x/xerrors"
)

// NotExistError is an error wrapper that gives the NotExist kind to an error.
type NotExistError struct {
	error
}

// Unwrap returns the next error in the error chain.
func (err *NotExistError) Unwrap() error {
	return err.error
}

func (err *NotExistError) Error() string {
	return fmt.Sprintf("not exists: %s", err.error)
}

// UnavailableError is an error wrapper that gives the Unavailable kind to an
// error.
type UnavailableError struct {
	error
}

// Unwrap returns the next error in the error chain.
func (err *UnavailableError) Unwrap() error {
	return err.error
}

func (err *UnavailableError) Error() string {
	return fmt.Sprintf("unavailable: %s", err.error)
}

// FailedPreconditionError is an error wrapper that gives the FailedPrecondition
// kind to an error.
type FailedPreconditionError struct {
	error
}

// Unwrap returns the next error in the error chain.
func (err *FailedPreconditionError) Unwrap() error {
	return err.error
}

func (err *FailedPreconditionError) Error() string {
	return fmt.Sprintf("failed precondition: %s", err.error)
}

// OtherError is an error wrapper that gives the Other kind to an error.
type OtherError struct {
	error
}

// Unwrap returns the next error in the error chain.
func (err *OtherError) Unwrap() error {
	return err.error
}

func (err *OtherError) Error() string {
	return fmt.Sprintf("%s", err.error)
}

// ErrNotPostgreSQLDriver is returned when an invalid database connection is
// passed to this locker client.
var ErrNotPostgreSQLDriver = xerrors.New("this is not a PostgreSQL connection")

// ErrNotAcquired indicates the given lock is already enforce to some other
// client.
var ErrNotAcquired = xerrors.New("cannot acquire lock")

// ErrLockAlreadyReleased indicates that a release call cannot be fulfilled
// because the client does not hold the lock
var ErrLockAlreadyReleased = xerrors.New("lock is already released")

// ErrLockNotFound is returned for get calls on missing lock entries.
var ErrLockNotFound = &NotExistError{xerrors.New("lock not found")}

// Validation errors
var (
	ErrDurationTooSmall = xerrors.New("Heartbeat period must be no more than half the length of the Lease Duration, " +
		"or locks might expire due to the heartbeat thread taking too long to update them (recommendation is to make it much greater, for example " +
		"4+ times greater)")
)
