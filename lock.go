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
	"sync"
	"time"
)

// Lock is the mutex entry in the database.
type Lock struct {
	client          *Client
	name            string
	heartbeatCancel context.CancelFunc
	leaseDuration   time.Duration

	replaceData     bool
	data            []byte
	failIfLocked    bool
	deleteOnRelease bool

	mu                  sync.Mutex
	isReleased          bool
	recordVersionNumber string
}

// Data returns the content of the lock, if any is available.
func (l *Lock) Data() []byte {
	return l.data
}

// Close releases the lock.
func (l *Lock) Close() error {
	return l.client.Release(l)
}

// IsReleased indicates whether the lock is either released or lost after
// heartbeat.
func (l *Lock) IsReleased() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.isReleased
}

// Option reconfigures how the lock behaves on acquire and release.
type Option func(*Lock)

// FailIfLocked will not retry to acquire the lock, instead returning.
func FailIfLocked(l *Lock) {
	l.failIfLocked = true
}

// WithData creates lock with data.
func WithData(data []byte) Option {
	return func(l *Lock) {
		l.data = data
	}
}

// DeleteOnRelease defines whether or not the lock should be deleted
// when Close() is called on the lock item.
func DeleteOnRelease(l *Lock) {
	l.deleteOnRelease = true
}

// ReplaceData will force the new content to be stored in the lock entry.
func ReplaceData(l *Lock) {
	l.replaceData = true
}
