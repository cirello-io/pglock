---
name: pglock-integration
description: Use when building or modifying a Go application that uses cirello.io/pglock for PostgreSQL-backed distributed locks, per-resource mutual exclusion, leader election, leases, heartbeats, lock metadata, or lock-aware workflows. Guide dependency and driver selection, schema provisioning, client configuration, acquisition and release, context cancellation, error handling, and integration tests. Do not use for changing pglock's own internals or for generic PostgreSQL administration.
license: Apache-2.0
compatibility: Requires a Go module compatible with the selected pglock version and PostgreSQL 15 or newer. The current package declares Go 1.25.0. Integration tests need a reachable PostgreSQL primary and permission to create or migrate the lock table and sequence.
metadata:
  repository: cirello.io/pglock
  purpose: third-party-application-integration
  version: "1"
---

# pglock Integration

Use this skill for application code that consumes `cirello.io/pglock`. Do not
modify the library's implementation or tests unless the user explicitly asks
for an upstream change. First identify the application operation that must be
exclusive, the stable lock key, the maximum hold time, the behavior on
contention, and what should happen if the database or lease becomes unhealthy.

## Integration Sequence

1. Add `cirello.io/pglock` to the application's Go module at the version the
   application supports. Use a PostgreSQL `*sql.DB`; keep one shared pool open
   for the application's clients.
2. Choose the driver constructor deliberately. `pglock.New` validates the
   connection as the `lib/pq` driver. If the application uses `pgx` through
   `github.com/jackc/pgx/v5/stdlib`, use `pglock.UnsafeNew` after verifying that
   the `*sql.DB` really targets PostgreSQL. Do not use `UnsafeNew` to bypass an
   unknown or non-PostgreSQL driver.
3. Provision the lock table and its `<table>_rvn` sequence once. Prefer a
   migration or `TryCreateTable` during controlled startup; use `CreateTable`
   when a duplicate table should be an error. Never drop the shared table from
   normal application startup.
4. Configure a single shared table and a stable owner identity for all
   contenders. Use `WithCustomTable` only with a trusted, validated identifier;
   the package interpolates that table name into SQL. Never derive it directly
   from user input.
5. Select lease and heartbeat values from the workload. A positive heartbeat
   must be no more than half the lease, and a lease at least four times the
   heartbeat is the safer starting point. The defaults are a 20-second lease
   and a 5-second heartbeat. Set the lease above normal critical-section time
   plus expected database latency, not merely above average execution time.
6. Use `AcquireContext` or `Do` with an application context, and always release
   an acquired lock. Make long-running callbacks stop when their callback
   context is canceled because that context is also canceled when heartbeat
   loss is detected.
7. Test contention and lease behavior against PostgreSQL. SQLite or an in-memory
   substitute cannot validate this package's PostgreSQL upsert, sequence,
   locking, and serialization behavior.

## Basic Bootstrap

For `lib/pq`, the safe constructor is:

```go
import (
	"context"
	"database/sql"
	"log"
	"time"

	"cirello.io/pglock"
	_ "github.com/lib/pq"
)

ctx := context.Background()
db, err := sql.Open("postgres", dsn)
if err != nil {
	log.Fatal(err)
}
if err := db.PingContext(ctx); err != nil {
	log.Fatal(err)
}

client, err := pglock.New(db,
	pglock.WithOwner(instanceID),
	pglock.WithLeaseDuration(20*time.Second),
	pglock.WithHeartbeatFrequency(5*time.Second),
)
if err != nil {
	log.Fatal(err)
}
if err := client.TryCreateTable(); err != nil {
	log.Fatal(err)
}
```

For `pgx` stdlib, open with the `pgx` driver and call `UnsafeNew`; `New` will
return `ErrNotPostgreSQLDriver` because it only recognizes `lib/pq`:

```go
import _ "github.com/jackc/pgx/v5/stdlib"

db, err := sql.Open("pgx", dsn)
client, err := pglock.UnsafeNew(db)
```

Check `err` after every setup call and close `db` only after all locks using it
have been released.

## Lock Patterns

Use a stable, bounded key shared by every process that must contend, such as
`campaign:<id>` or `leader:<service>`. The schema stores names and owners in
`VARCHAR(255)`, so bound or hash longer application identifiers before passing
them to pglock.

For a bounded critical section:

```go
import (
	"errors"
	"log"
)

lock, err := client.AcquireContext(ctx, lockKey)
if err != nil {
	if errors.Is(err, pglock.ErrNotAcquired) {
		// The context ended before acquisition, or use FailIfLocked below.
	}
	return err
}
defer func() {
	if releaseErr := lock.Close(); releaseErr != nil &&
		!errors.Is(releaseErr, pglock.ErrLockAlreadyReleased) {
		log.Printf("release %q: %v", lockKey, releaseErr)
	}
}()

// Do the exclusive work while the lock is held.
```

- `Acquire` waits until the key is available. Use `FailIfLocked()` when a
  caller should get `ErrNotAcquired` immediately instead of waiting.
- `AcquireContext` returns `ErrNotAcquired` when its context is already done or
  ends before acquisition. Give it a deadline when a request cannot wait
  indefinitely.
- `Do` acquires, invokes `func(context.Context, *pglock.Lock) error`, cancels
  that callback context on heartbeat loss, and releases on return. Callback
  code must select on `ctx.Done()` around long work and must not continue
  externally visible side effects after losing the lock.
- `KeepOnRelease()` retains the row after release. Combine it with `WithData`
  when lock metadata should survive ownership changes. Use `ReplaceData()` on a
  later acquisition when retained data must be replaced; otherwise existing
  data is reused.
- `Get`, `GetData`, and `GetAllLocks` are inspection APIs. They do not acquire
  ownership and must not be used as an authorization check for protected work.
- `WithOwner` makes the current process or instance visible to `Get` and
  `GetAllLocks`. Use a stable, non-secret identifier, not a password or token.

## Errors And Operations

Use `errors.Is` and `errors.As`, never error-string matching:

- `ErrNotAcquired`: contention or acquisition context ended.
- `ErrLockAlreadyReleased`: the lock was already released or lost; a deferred
  close may treat this as an expected cleanup result.
- `ErrLockNotFound` and `NotExistError`: an inspection target is absent.
- `ErrDurationTooSmall`: the heartbeat is too slow relative to the lease.
- `ErrNotPostgreSQLDriver`: `New` received a driver other than `lib/pq`.
- `UnavailableError`, `FailedPreconditionError`, and `OtherError` classify
  wrapped database failures. The client retries PostgreSQL serialization
  failures (`SQLSTATE 40001`) internally, but application work must still be
  idempotent if the caller chooses to retry after a returned error.

Point the client at the PostgreSQL primary, not a read replica. Every contender
must reach the same database and lock table. Grant the application the DDL
permissions needed for one-time setup, or run the equivalent `schema.sql`
migration under a database owner and grant runtime DML/sequence access. Keep
credentials in the application's normal secret/configuration path.

## Integration Tests

At minimum, run these scenarios against a real PostgreSQL service:

1. Two clients acquire the same key; the first owns it and a second client with
   `FailIfLocked()` receives `ErrNotAcquired`.
2. Release the first lock and verify a second client can acquire the same key.
3. Cancel or time out a waiting `AcquireContext` and verify it does not enter
   the critical section.
4. Hold a lock longer than one heartbeat interval and verify the owner remains
   valid. Use a lease/heartbeat pair that keeps the test deterministic.
5. For `Do`, cancel the callback context and verify the callback exits and the
   lock becomes available to another client.
6. If using lock data or custom owners, read them with `Get`/`GetData` and test
   `KeepOnRelease` and `ReplaceData` explicitly.

Use unique trusted test table names or an isolated database, clean up with
`DropTable` only in test teardown, and do not let parallel tests share a fixed
table/key unless the contention is intentional. Run the application race
detector as well, but do not treat `-race` without PostgreSQL as proof of
distributed-lock correctness.

## Bundled Resources

- Read `references/integration-guide.md` for the API matrix, schema shape, and
  failure-mode details when the integration spans multiple components.
- Run `python3 scripts/validate_skill.py --help` for the dependency-free skill
  validator. It emits structured JSON and never prompts.
- Use `evals/evals.json` for application-level output evals and
  `evals/trigger_queries.json` for description-trigger evaluation. Keep the
  trigger train/validation split fixed when measuring a compatible agent
  client's activation rate.
