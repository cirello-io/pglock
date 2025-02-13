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
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"strings"
	"text/template"
	"time"

	"github.com/lib/pq"
)

// DefaultTableName defines the table which the client is going to use to store
// the content and the metadata of the locks. Use WithCustomTable to modify this
// value.
const DefaultTableName = "locks"

// DefaultLeaseDuration is the recommended period of time that a lock can be
// considered valid before being stolen by another client. Use WithLeaseDuration
// to modify this value.
const DefaultLeaseDuration = 20 * time.Second

// DefaultHeartbeatFrequency is the recommended frequency that client should
// refresh the lock so to avoid other clients from stealing it. Use
// WithHeartbeatFrequency to modify this value.
const DefaultHeartbeatFrequency = 5 * time.Second

// Client is the PostgreSQL's backed distributed lock. Make sure it is always
// configured to talk to leaders and not followers in the case of replicated
// setups.
type Client struct {
	db                 *sql.DB
	tableName          string
	leaseDuration      time.Duration
	heartbeatFrequency time.Duration
	log                LevelLogger
	owner              string
}

// New returns a locker client from the given database connection. This function
// validates that *sql.DB holds a ratified postgreSQL driver (lib/pq).
func New(db *sql.DB, opts ...ClientOption) (*Client, error) {
	if db == nil {
		return nil, ErrNotPostgreSQLDriver
	} else if _, ok := db.Driver().(*pq.Driver); !ok {
		return nil, ErrNotPostgreSQLDriver
	}
	return newClient(db, opts...)
}

// UnsafeNew returns a locker client from the given database connection. This
// function does not check if *sql.DB holds a ratified postgreSQL driver.
func UnsafeNew(db *sql.DB, opts ...ClientOption) (*Client, error) {
	if db == nil {
		return nil, ErrNotPostgreSQLDriver
	}
	return newClient(db, opts...)
}

func newClient(db *sql.DB, opts ...ClientOption) (*Client, error) {
	c := &Client{
		db:                 db,
		tableName:          DefaultTableName,
		leaseDuration:      DefaultLeaseDuration,
		heartbeatFrequency: DefaultHeartbeatFrequency,
		log:                &flatLogger{log.New(io.Discard, "", 0)},
		owner:              fmt.Sprintf("pglock-%v", rand.Int()),
	}
	for _, opt := range opts {
		opt(c)
	}
	if isDurationTooSmall(c) {
		db.Close()
		return nil, ErrDurationTooSmall
	}
	return c, nil
}

func isDurationTooSmall(c *Client) bool {
	return c.heartbeatFrequency > 0 && c.leaseDuration < 2*c.heartbeatFrequency
}

func (c *Client) newLock(ctx context.Context, name string, opts []LockOption) *Lock {
	heartbeatContext, heartbeatCancel := context.WithCancel(ctx)
	l := &Lock{
		client:           c,
		name:             name,
		leaseDuration:    c.leaseDuration,
		heartbeatContext: heartbeatContext,
		heartbeatCancel:  heartbeatCancel,
	}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

var createTableSchemaCommands = []*template.Template{
	template.Must(template.New("createTable").Parse(`CREATE TABLE {{.Modifier}} {{.TableName}} (
	name CHARACTER VARYING(255) PRIMARY KEY,
	record_version_number BIGINT,
	data BYTEA,
	owner CHARACTER VARYING(255)
)`)),
	template.Must(template.New("createSequence").Parse(`CREATE SEQUENCE {{.Modifier}} {{.TableName}}_rvn CYCLE OWNED BY {{.TableName}}.record_version_number`)),
}

// CreateTable prepares a PostgreSQL table with the right DDL for it to be used
// by this lock client. If the table already exists, it will return an error.
func (c *Client) CreateTable() error {
	values := createTableTemplateValue{TableName: c.tableName, Modifier: ""}
	return c.createTable(values)
}

// TryCreateTable prepares a PostgreSQL table with the right DDL for it to be
// used by this lock client. If the table already exists, it will be a no-op.
func (c *Client) TryCreateTable() error {
	values := createTableTemplateValue{TableName: c.tableName, Modifier: "IF NOT EXISTS"}
	return c.createTable(values)
}

type createTableTemplateValue struct {
	TableName string
	Modifier  string
}

func (c *Client) createTable(values createTableTemplateValue) error {
	for _, cmd := range createTableSchemaCommands {
		var qry strings.Builder
		_ = cmd.Execute(&qry, values)
		if _, err := c.db.Exec(qry.String()); err != nil {
			return fmt.Errorf("cannot setup the database: %w", err)
		}
	}
	return nil
}

// DropTable cleans up a PostgreSQL DB from what was created in the CreateTable
// function.
func (c *Client) DropTable() error {
	_, err := c.db.Exec("DROP TABLE " + c.tableName)
	if err != nil {
		return fmt.Errorf("cannot cleanup the database: %w", err)
	}
	return nil
}

// Acquire attempts to grab the lock with the given key name and wait until it
// succeeds.
func (c *Client) Acquire(name string, opts ...LockOption) (*Lock, error) {
	return c.AcquireContext(context.Background(), name, opts...)
}

// AcquireContext attempts to grab the lock with the given key name, wait until
// it succeeds or the context is done. It returns ErrNotAcquired if the context
// is canceled before the lock is acquired.
func (c *Client) AcquireContext(ctx context.Context, name string, opts ...LockOption) (*Lock, error) {
	l := c.newLock(ctx, name, opts)
	for {
		if err := ctx.Err(); err != nil {
			return nil, ErrNotAcquired
		}
		err := c.retry(func() error { return c.tryAcquire(ctx, l) })
		switch {
		case l.failIfLocked && errors.Is(err, ErrNotAcquired):
			c.log.Debug("not acquired, exit")
			return l, err
		case errors.Is(err, ErrNotAcquired):
			c.log.Debug("not acquired, wait: %v", l.leaseDuration)
			waitFor(ctx, l.leaseDuration)
			continue
		case err != nil:
			c.log.Error("error: %v", err)
			return nil, err
		}
		return l, nil
	}
}

func (c *Client) tryAcquire(ctx context.Context, l *Lock) error {
	err := c.storeAcquire(ctx, l)
	if err != nil {
		return err
	}
	if c.heartbeatFrequency > 0 {
		l.heartbeatWG.Add(1)
		go func() {
			defer l.heartbeatCancel()
			c.heartbeat(l.heartbeatContext, l)
		}()
	}
	return nil
}

func (c *Client) storeAcquire(ctx context.Context, l *Lock) error {
	ctx, cancel := context.WithTimeout(ctx, l.leaseDuration)
	defer cancel()

	rvn, err := c.getNextRVN(ctx, c.db)
	if err != nil {
		return typedError(err, "cannot run query to read record version number")
	}

	c.log.Debug("storeAcquire in: %v %v %v %v", l.name, rvn, l.data, l.recordVersionNumber)
	defer func() {
		c.log.Debug("storeAcquire out: %v %v %v %v", l.name, rvn, l.data, l.recordVersionNumber)
	}()
	rowLockInfo := c.db.QueryRowContext(ctx, `
		INSERT INTO `+c.tableName+`
			("name", "record_version_number", "data", "owner")
		VALUES
			($1, $2, $3, $6)
		ON CONFLICT ("name") DO UPDATE
		SET
			"record_version_number" = CASE
				WHEN COALESCE(`+c.tableName+`."record_version_number" = $4, TRUE) THEN $2
				ELSE `+c.tableName+`."record_version_number"
			END,
			"data" = CASE
				WHEN COALESCE(`+c.tableName+`."record_version_number" = $4, TRUE) THEN
					CASE
						WHEN $5 THEN $3
						ELSE `+c.tableName+`."data"
					END
				ELSE `+c.tableName+`."data"
			END,
			"owner" = CASE
				WHEN COALESCE(`+c.tableName+`."record_version_number" = $4, TRUE) THEN $6
				ELSE `+c.tableName+`."owner"
			END
		RETURNING
			"record_version_number", "data", "owner"
	`, l.name, rvn, l.data, l.recordVersionNumber, l.replaceData, c.owner)
	var (
		actualRVN   int64
		actualData  []byte
		actualOwner string
	)
	if err := rowLockInfo.Scan(&actualRVN, &actualData, &actualOwner); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return typedError(err, "cannot load information for lock acquisition")
	}
	l.owner = actualOwner
	if actualRVN != rvn {
		l.recordVersionNumber = actualRVN
		return ErrNotAcquired
	}
	l.recordVersionNumber = rvn
	l.data = actualData
	return nil
}

// Do executes f while holding the lock for the named lock. When the lock loss
// is detected in the heartbeat, it is going to cancel the context passed on to
// f. If it ends normally (err == nil), it releases the lock.
func (c *Client) Do(ctx context.Context, name string, f func(context.Context, *Lock) error, opts ...LockOption) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	l, err := c.AcquireContext(ctx, name, opts...)
	if err != nil {
		return err
	}
	defer l.Close()
	go func() {
		// In the hierarchy of context cancelations, it might be the
		// case that the heartbeat context has been canceled, even
		// though the parent is still intact. This trap will bubble
		// children cancellations up.
		<-l.heartbeatContext.Done()
		cancel()
	}()
	return f(ctx, l)
}

// Release will update the mutex entry to be able to be taken by other clients.
func (c *Client) Release(l *Lock) error {
	return c.ReleaseContext(context.Background(), l)
}

// ReleaseContext will update the mutex entry to be able to be taken by other
// clients. If a heartbeat is running, it will stopped it.
func (c *Client) ReleaseContext(ctx context.Context, l *Lock) error {
	l.heartbeatCancel()
	l.heartbeatWG.Wait()
	err := c.retry(func() error { return c.storeRelease(ctx, l) })
	return err
}

func (c *Client) storeRelease(ctx context.Context, l *Lock) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, l.leaseDuration)
	defer cancel()
	var result sql.Result
	switch l.keepOnRelease {
	case true:
		res, err := c.db.ExecContext(ctx, `
			UPDATE
				`+c.tableName+`
			SET
				"record_version_number" = NULL
			WHERE
				"name" = $1
				AND "record_version_number" = $2
		`, l.name, l.recordVersionNumber)
		if err != nil {
			return typedError(err, "cannot run query to release lock (keep)")
		}
		result = res
	case false:
		res, err := c.db.ExecContext(ctx, `
			DELETE FROM
				`+c.tableName+`
			WHERE
				"name" = $1
				AND "record_version_number" = $2
		`, l.name, l.recordVersionNumber)
		if err != nil {
			return typedError(err, "cannot run query to delete lock (delete)")
		}
		result = res
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return typedError(err, "cannot confirm whether the lock has been released")
	} else if affected == 0 {
		l.isReleased = true
		return ErrLockAlreadyReleased
	}
	l.isReleased = true
	l.heartbeatCancel()
	return nil
}

func (c *Client) heartbeat(ctx context.Context, l *Lock) {
	defer l.heartbeatWG.Done()
	c.log.Debug("heartbeat started: %v", l.name)
	defer c.log.Debug("heartbeat stopped: %v", l.name)
	for {
		if err := c.SendHeartbeat(ctx, l); err != nil && !isContextError(err) {
			defer c.log.Error("heartbeat missed: %v", err)
			return
		} else if err := ctx.Err(); err != nil {
			return
		}
		waitFor(ctx, c.heartbeatFrequency)
	}
}

// SendHeartbeat refreshes the mutex entry so to avoid other clients from
// grabbing it.
func (c *Client) SendHeartbeat(ctx context.Context, l *Lock) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.isReleased {
		return ErrLockAlreadyReleased
	}
	err := c.retry(func() error { return c.storeHeartbeat(ctx, l) })
	if err != nil {
		l.isReleased = true
		return fmt.Errorf("cannot send heartbeat (%v): %w", l.name, err)
	}
	return nil
}

func (c *Client) storeHeartbeat(ctx context.Context, l *Lock) error {
	ctx, cancel := context.WithTimeout(ctx, l.leaseDuration)
	defer cancel()
	rvn, err := c.getNextRVN(ctx, c.db)
	if err != nil {
		return typedError(err, "cannot run query to read record version number")
	}
	result, err := c.db.ExecContext(ctx, `
		UPDATE
			`+c.tableName+`
		SET
			"record_version_number" = $3
		WHERE
			"name" = $1
			AND "record_version_number" = $2
	`, l.name, l.recordVersionNumber, rvn)
	if err != nil {
		return typedError(err, "cannot run query to update the heartbeat")
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return typedError(err, "cannot confirm whether the lock has been updated for the heartbeat")
	} else if affected == 0 {
		return ErrLockAlreadyReleased
	}
	l.recordVersionNumber = rvn
	return nil
}

// GetData returns the data field from the given lock in the table without
// holding the lock first.
func (c *Client) GetData(name string) ([]byte, error) {
	return c.GetDataContext(context.Background(), name)
}

// Get returns the lock object from the given name in the table without holding
// it first.
func (c *Client) Get(name string) (*Lock, error) {
	return c.GetContext(context.Background(), name)
}

// GetDataContext returns the data field from the given lock in the table
// without holding the lock first.
func (c *Client) GetDataContext(ctx context.Context, name string) ([]byte, error) {
	l, err := c.GetContext(ctx, name)
	return l.Data(), err
}

// GetContext returns the lock object from the given name in the table without
// holding it first.
func (c *Client) GetContext(ctx context.Context, name string) (*Lock, error) {
	var l *Lock
	err := c.retry(func() error {
		var err error
		l, err = c.getLock(ctx, name)
		return err
	})
	if notExist := (&NotExistError{}); err != nil && errors.As(err, &notExist) {
		c.log.Error("missing lock entry: %v", err)
	}
	return l, err
}

func (c *Client) getLock(ctx context.Context, name string) (*Lock, error) {
	ctx, cancel := context.WithTimeout(ctx, c.leaseDuration)
	defer cancel()
	row := c.db.QueryRowContext(ctx, `
		SELECT
			"name", "owner", "data"
		FROM
			`+c.tableName+`
		WHERE
			"name" = $1
		FOR UPDATE
	`, name)
	l := c.newLock(ctx, name, nil)
	l.isReleased = true
	l.recordVersionNumber = -1
	err := row.Scan(&l.name, &l.owner, &l.data)
	if err == sql.ErrNoRows {
		return l, ErrLockNotFound
	}
	return l, typedError(err, "cannot load the data of this lock")
}

func (c *Client) getNextRVN(ctx context.Context, db *sql.DB) (int64, error) {
	rowRVN := db.QueryRowContext(ctx, `SELECT nextval('`+c.tableName+`_rvn')`)
	var rvn int64
	err := rowRVN.Scan(&rvn)
	return rvn, err
}

const maxRetries = 1024

func (c *Client) retry(f func() error) error {
	var err error
	for i := 0; i < maxRetries; i++ {
		err = f()
		if failedPrecondition := (&FailedPreconditionError{}); err == nil || !errors.As(err, &failedPrecondition) {
			break
		}
		c.log.Debug("bad transaction, retrying: %v", err)
		if isContextError(err) {
			break
		}
	}
	return err
}

// GetAllLocks returns all known locks in a read-only fashion.
func (c *Client) GetAllLocks() ([]*ReadOnlyLock, error) {
	return c.GetAllLocksContext(context.Background())
}

// GetAllLocksContext returns all known locks in a read-only fashion.
func (c *Client) GetAllLocksContext(ctx context.Context) ([]*ReadOnlyLock, error) {
	var locks []*ReadOnlyLock
	err := c.retry(func() error {
		var err error
		locks, err = c.getAllLocks(ctx)
		return err
	})
	return locks, err
}

func (c *Client) getAllLocks(ctx context.Context) ([]*ReadOnlyLock, error) {
	ctx, cancel := context.WithTimeout(ctx, c.leaseDuration)
	defer cancel()
	rows, err := c.db.QueryContext(ctx, `SELECT "name", "owner", "data" FROM `+c.tableName)
	if err != nil {
		return nil, typedError(err, "cannot query all locks")
	}
	defer rows.Close()
	var locks []*ReadOnlyLock
	for rows.Next() {
		lock := &ReadOnlyLock{}
		if err := rows.Scan(&lock.name, &lock.owner, &lock.data); err != nil {
			return nil, typedError(err, "cannot scan row")
		}
		locks = append(locks, lock)
	}
	if err := rows.Err(); err != nil {
		return nil, typedError(err, "failed to scan rows")
	}
	return locks, nil
}

// ClientOption reconfigures the lock client.
type ClientOption func(*Client)

// WithLogger injects a logger into the client, so its internals can be
// recorded.
// Deprecated: Use WithLevelLogger instead.
func WithLogger(l Logger) ClientOption {
	return func(c *Client) { c.log = &flatLogger{l} }
}

// WithLevelLogger injects a logger into the client, so its internals can be
// recorded.
func WithLevelLogger(l LevelLogger) ClientOption {
	return func(c *Client) { c.log = l }
}

// WithLeaseDuration defines how long should the lease be held.
func WithLeaseDuration(d time.Duration) ClientOption {
	return func(c *Client) { c.leaseDuration = d }
}

// WithHeartbeatFrequency defines the frequency of the heartbeats. Heartbeats
// should have no more than half of the duration of the lease.
func WithHeartbeatFrequency(d time.Duration) ClientOption {
	return func(c *Client) { c.heartbeatFrequency = d }
}

// WithCustomTable reconfigures the lock client to use an alternate lock table
// name.
func WithCustomTable(tableName string) ClientOption {
	return func(c *Client) { c.tableName = tableName }
}

// WithOwner reconfigures the lock client to use a custom owner name.
func WithOwner(owner string) ClientOption {
	return func(c *Client) { c.owner = owner }
}

func typedError(err error, msg string) error {
	const serializationErrorCode = "40001"
	if err == nil {
		return nil
	} else if errors.Is(err, sql.ErrNoRows) {
		return &NotExistError{fmt.Errorf(msg+": %w", err)}
	} else if errNetOp := (&net.OpError{}); errors.As(err, &errNetOp) {
		return &UnavailableError{fmt.Errorf(msg+": %w", err)}
	} else if errPQ := (&pq.Error{}); errors.As(err, &errPQ) && errPQ.Code == serializationErrorCode {
		return &FailedPreconditionError{fmt.Errorf(msg+": %w", err)}
	} else if e, ok := unwrapUntilSQLState(err); ok && e.SQLState() == serializationErrorCode {
		return &FailedPreconditionError{fmt.Errorf(msg+": %w", err)}
	}
	return &OtherError{err}
}

func unwrapUntilSQLState(err error) (interface{ SQLState() string }, bool) {
	for {
		if e, ok := hasSQLState(err); ok {
			return e, true
		}
		err = errors.Unwrap(err)
		if err == nil {
			return nil, false
		}
	}
}

func hasSQLState(v any) (interface{ SQLState() string }, bool) {
	sqlState, ok := v.(interface{ SQLState() string })
	return sqlState, ok
}

func waitFor(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
