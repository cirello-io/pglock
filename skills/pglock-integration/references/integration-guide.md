# pglock Integration Guide

This reference expands the application-facing contracts in `SKILL.md`. It is
about consuming the package from another Go program, not maintaining the
package itself.

## API Matrix

| Need | API | Application behavior |
| --- | --- | --- |
| Construct with `lib/pq` | `pglock.New(db, opts...)` | Use `_ "github.com/lib/pq"`; check `ErrNotPostgreSQLDriver`. |
| Construct with another PostgreSQL `database/sql` driver | `pglock.UnsafeNew(db, opts...)` | Verify the driver yourself; this bypasses only pglock's driver type assertion. |
| One-time setup that must fail on duplicates | `CreateTable()` | Use in an explicit migration/setup step. |
| Idempotent setup | `TryCreateTable()` | Safe for controlled startup when the existing table has the expected shape. |
| Blocking acquisition | `Acquire` / `AcquireContext` | Always give `AcquireContext` a bounded context for request-scoped work. |
| Non-blocking acquisition | `FailIfLocked()` | Treat `ErrNotAcquired` as normal contention, not necessarily a system fault. |
| Long-running ownership | `Do(ctx, name, callback, opts...)` | Callback must exit on its context; heartbeat loss cancels it. |
| Release | `Lock.Close`, `Release`, `ReleaseContext` | Stop work before releasing; accept `ErrLockAlreadyReleased` during cleanup when appropriate. |
| Retained metadata | `KeepOnRelease`, `WithData`, `ReplaceData` | Existing data is retained unless `ReplaceData` is passed on reacquisition. |
| Inspection | `Get`, `GetData`, `GetAllLocks` | These observe rows; they do not grant ownership. |
| Diagnostics | `WithOwner`, `WithLevelLogger` | Owner is stored metadata; the default logger discards internal logs. |

## Schema Contract

The default table is `locks` and the default sequence is `locks_rvn`:

```sql
CREATE TABLE IF NOT EXISTS locks (
    name CHARACTER VARYING(255) PRIMARY KEY,
    record_version_number BIGINT,
    data BYTEA,
    owner CHARACTER VARYING(255)
);
CREATE SEQUENCE IF NOT EXISTS locks_rvn CYCLE OWNED BY locks.record_version_number;
```

For `WithCustomTable("app_locks")`, the package expects the equivalent table
and the `app_locks_rvn` sequence. The table name is concatenated into SQL, so
configure it from a trusted constant or validate it as a PostgreSQL identifier.
Lock names and owner values are bounded by the 255-character columns. The
`data` field is binary `BYTEA` and is optional.

Do not create one table per process. All contenders for a lock must use the
same PostgreSQL primary, table, and sequence. Run schema creation through a
migration when the application cannot safely hold DDL permissions at runtime.

## Timing Model

The package does not store wall-clock expiry timestamps. Each acquisition and
heartbeat advances a record version number. A client considers its lock valid
only while the database row still has the version it expects. A client that
stops heartbeating can be replaced after the lease duration.

`WithHeartbeatFrequency(d)` is enabled only when `d > 0`. Construction rejects
configurations where the lease is less than twice the heartbeat. Use a larger
ratio in production, account for database latency and transient load, and set
the critical-section context deadline consistently with the lease. Disabling
heartbeats is appropriate only when the hold time is safely below the lease.

## Driver Trap

`New` checks `db.Driver()` for `*pq.Driver`, not merely for a PostgreSQL DSN.
This fails with a `pgx` stdlib connection even though `pgx` is PostgreSQL. The
two supported integration shapes are:

```go
// lib/pq
db, err := sql.Open("postgres", dsn)
client, err := pglock.New(db)

// pgx stdlib
db, err := sql.Open("pgx", dsn)
client, err := pglock.UnsafeNew(db)
```

In both cases, call `PingContext` before declaring the application ready and
close the `*sql.DB` only during application shutdown after held locks are done.

## Failure Handling

`ErrNotAcquired` is the normal signal for a canceled wait or `FailIfLocked`
contention. `ErrLockAlreadyReleased` can indicate a repeated close, a lost
heartbeat, or a row changed by another owner; do not continue protected work
after observing that the lock is released. `ErrLockNotFound` applies to
inspection calls and is wrapped as `NotExistError`.

The client retries serialization failures with SQLSTATE `40001`. Other
database failures are wrapped as `UnavailableError`, `FailedPreconditionError`,
or `OtherError`. Preserve those chains with `%w` in application errors and
classify them with `errors.Is`/`errors.As`. Retrying an application callback is
safe only when its side effects are idempotent or separately deduplicated.
