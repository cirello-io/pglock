/*
Copyright 2019 github.com/ucirello & cirello.io

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
	"math/rand"
	"testing"
	"time"
)

var chars = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func init() {
	rand.Seed(time.Now().UnixNano())
}

func randStr() string {
	const length = 32
	var b bytes.Buffer
	for i := 0; i < length; i++ {
		b.WriteByte(chars[rand.Intn(len(chars))])
	}
	return b.String()
}

type testLogger struct {
	t testing.TB
}

func (t *testLogger) Println(v ...interface{}) {
	t.t.Helper()
	t.t.Log(v...)
}

type discardLogging struct{}

func (t *discardLogging) Println(...interface{}) {}

type testLevelLogger struct {
	t testing.TB
}

func (t *testLevelLogger) Debug(msg string, args ...any) {
	t.t.Helper()
	t.t.Logf("DEBUG: "+msg, args...)
}

func (t *testLevelLogger) Error(msg string, args ...any) {
	t.t.Helper()
	t.t.Logf("ERROR: "+msg, args...)
}
