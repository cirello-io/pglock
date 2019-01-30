# PostgreSQL Lock Client for Go

[![GoDoc](https://godoc.org/cirello.io/pglock?status.svg)](https://godoc.org/cirello.io/pglock)
[![Build Status](https://travis-ci.org/cirello-io/pglock.svg?branch=master)](https://travis-ci.org/cirello-io/pglock)
[![Coverage Status](https://coveralls.io/repos/github/cirello-io/pglock/badge.svg?branch=master)](https://coveralls.io/github/cirello-io/pglock?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/cirello-io/pglock)](https://goreportcard.com/report/github.com/cirello-io/pglock)
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go)
[![SLA](https://img.shields.io/badge/SLA-95%25-brightgreen.svg)](https://github.com/cirello-io/public/blob/master/SLA.md)

The PostgreSQL Lock Client for Go is a general purpose distributed locking
library built for PostgreSQL. The PostgreSQL Lock Client for Go supports both
fine-grained and coarse-grained locking as the lock keys can be any arbitrary
string, up to a certain length. Please create issues in the GitHub repository
with questions, pull request are very much welcome.

_Recommended PostgreSQL version: 9.6 or newer_

## Use cases
A common use case for this lock client is:
let's say you have a distributed system that needs to periodically do work on a
given campaign (or a given customer, or any other object) and you want to make
sure that two boxes don't work on the same campaign/customer at the same time.
An easy way to fix this is to write a system that takes a lock on a customer,
but fine-grained locking is a tough problem. This library attempts to simplify
this locking problem on top of PostgreSQL.

Another use case is leader election. If you only want one host to be the leader,
then this lock client is a great way to pick one. When the leader fails, it will
fail over to another host within a customizable lease duration that you set.

## Getting Started
To use the PostgreSQL Lock Client for Go, you must make it sure it is present in
`$GOPATH` or in your vendor directory.

```sh
$ go get -u cirello.io/pglock
```

This package has the `go.mod` file to be used with Go's module system. If you
need to work on this package, use `go mod edit -replace=cirello.io/pglock@yourlocalcopy`.

For your convenience, there is a function in the package called `CreateTable`
that you can use to set up your table. The package level documentation comment
has an example of how to use this package. Here is some example code to get you
started:

```Go
package main

import (
	"log"

	"cirello.io/pglock"
)

func main() {
	db, err := sql.Open("postgres", *dsn)
	if err != nil {
		t.Fatal("cannot connect to test database server:", err)
	}
	c, err := pglock.New(db,
		pglock.WithLeaseDuration(3*time.Second),
		pglock.WithHeartbeatFrequency(1*time.Second),
	)
	if err != nil {
		t.Fatal("cannot create lock client:", err)
	}
	l, err := c.Acquire("lock-name")
	if err != nil {
		t.Fatal("unexpected error while acquiring 1st lock:", err)
	}
	defer l.Close()
	// execute the logic
}
```

## Selected Features
### Send Automatic Heartbeats
When you create the lock client, you can specify `WithHeartbeatFrequency(time.Duration)`
like in the above example, and it will spawn a background goroutine that
continually updates the record version number on your locks to prevent them from
expiring (it does this by calling the `SendHeartbeat()` method in the lock
client.) This will ensure that as long as your application is running, your
locks will not expire until you call `Release()` or `lockItem.Close()`

### Read the data in a lock without acquiring it
You can read the data in the lock without acquiring it. Here's how:
```Go
lock, err := lockClient.Get("kirk");
```

## Logic to avoid problems with clock skew
The lock client never stores absolute times in PostgreSQL. The way locks are
expired is that a call to tryAcquire reads in the current lock, checks the
record version number of the lock and starts a timer. If the lock still has the
same after the lease duration time has passed, the client will determine that
the lock is stale and expire it.

What this means is that, even if two different machines disagree about what time
it is, they will still avoid clobbering each other's locks.
