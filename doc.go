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

// Package pglock provides a simple utility for using PostgreSQL for managing
// distributed locks.
//
// In order to use this package, the client must create a table in the database,
// although the client provides a convenience method for creating that table
// (CreateTable).
//
// Basic usage:
//
// 	db, err := sql.Open("postgres", *dsn)
// 	if err != nil {
// 		log.Fatal("cannot connect to test database server:", err)
// 	}
// 	name := randStr(32)
// 	c, err := pglock.New(db)
// 	if err != nil {
// 		log.Fatal("cannot create lock client:", err)
// 	}
// 	l1, err := c.Acquire(name)
// 	if err != nil {
// 		log.Fatal("unexpected error while acquiring 1st lock:", err)
// 	}
// 	t.Log("acquired first lock")
// 	var wg sync.WaitGroup
// 	wg.Add(1)
// 	var locked bool
// 	go func() {
// 		defer wg.Done()
// 		l2, err := c.Acquire(name)
// 		if err != nil {
// 			log.Fatal("unexpected error while acquiring 2nd lock:", err)
// 		}
// 		t.Log("acquired second lock")
// 		locked = true
// 		l2.Close()
// 	}()
// 	time.Sleep(6 * time.Second)
// 	l1.Close()
// 	wg.Wait()
//
// pglock.Client.Do can be used for long-running processes:
//
// 	err = c.Do(context.Background(), name, func(ctx context.Context, l *pglock.Lock) error {
// 		once := make(chan struct{}, 1)
// 		once <- struct{}{}
// 		for {
// 			select {
// 			case <-ctx.Done():
// 				t.Log("context canceled")
// 				return ctx.Err()
// 			case <-once:
// 				t.Log("executed once")
// 				close(ranOnce)
// 			}
// 		}
// 	})
// 	if err != nil && err != context.Canceled {
// 		log.Fatal("unexpected error while running under lock:", err)
// 	}
//
//
// This package is covered by this SLA:
// https://github.com/cirello-io/public/blob/master/SLA.md
//
package pglock // import "cirello.io/pglock"
