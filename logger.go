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

import "fmt"

// Logger is used for internal inspection of the lock client.
type Logger interface {
	Println(v ...any)
}

// LevelLogger is used for internal inspection of the lock client.
type LevelLogger interface {
	Debug(msg string, args ...any)
	Error(msg string, args ...any)
}

type flatLogger struct {
	l Logger
}

func (fl *flatLogger) Debug(msg string, args ...any) {
	fl.l.Println(fmt.Sprintf(msg, args...))
}

func (fl *flatLogger) Error(msg string, args ...any) {
	fl.l.Println(fmt.Sprintf(msg, args...))
}
