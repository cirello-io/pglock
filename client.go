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
	"io/ioutil"
	"log"
	"time"

	"cirello.io/errors"
	"github.com/davecgh/go-spew/spew"
	"github.com/lib/pq"
)

const (
	defaultTableName          = "locks"
	defaultLeaseDuration      = 20 * time.Second
	defaultHeartbeatFrequency = 5 * time.Second
)

// ErrNotPostgreSQLDriver is returned when an invalid database connection is
// passed to this locker client.
var ErrNotPostgreSQLDriver = errors.E("this is not a PostgreSQL connection")

// ErrNotAcquired indicates the given lock is already enforce to some other
// client.
var ErrNotAcquired = errors.E("cannot acquire lock")

// ErrLockAlreadyReleased indicates that a release call cannot be fulfilled
// because the client does not hold the lock
var ErrLockAlreadyReleased = errors.E("lock is already released")

// ErrLockNotFound is returned for get calls on missing lock entries.
var ErrLockNotFound = errors.E(errors.NotExist, "lock not found")

// Validation errors
var (
	ErrDurationTooSmall = errors.E("Heartbeat period must be no more than half the length of the Lease Duration, " +
		"or locks might expire due to the heartbeat thread taking too long to update them (recommendation is to make it much greater, for example " +
		"4+ times greater)")
	ErrInvalidHeartbeatFrequency = errors.E("heartbeat frequency must be bigger than 0")
)

// Client is the PostgreSQL's backed distributed lock. Make sure it is always
// configured to talk to leaders and not followers in the case of replicated
// setups.
type Client struct {
	db                 *sql.DB
	tableName          string
	leaseDuration      time.Duration
	heartbeatFrequency time.Duration
	log                Logger
}

// New returns a locker client from the given database connection.
func New(db *sql.DB, opts ...ClientOption) (*Client, error) {
	if _, ok := db.Driver().(*pq.Driver); !ok {
		return nil, ErrNotPostgreSQLDriver
	}
	c := &Client{
		db:                 db,
		tableName:          defaultTableName,
		leaseDuration:      defaultLeaseDuration,
		heartbeatFrequency: defaultHeartbeatFrequency,
		log:                log.New(ioutil.Discard, "", 0),
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.heartbeatFrequency <= 0 {
		return nil, ErrInvalidHeartbeatFrequency
	}
	if c.leaseDuration < 2*c.heartbeatFrequency {
		return nil, ErrDurationTooSmall
	}
	return c, nil
}

func (c *Client) newLock(name string, opts []Option) *Lock {
	l := &Lock{
		client:        c,
		name:          name,
		leaseDuration: c.leaseDuration,
	}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

// CreateTable prepares a PostgreSQL table with the right DDL for it to be used
// by this lock client. If the table already exists, it will return an error.
func (c *Client) CreateTable() error {
	_, err := c.db.Exec(`CREATE TABLE ` + c.tableName + ` (
		name character varying(255) PRIMARY KEY,
		record_version_number character varying(255),
		data bytea
	);`)
	return errors.E(err, "cannot create the table")
}

// Acquire attempts to grab the lock with the given key name, wait until it
// succeeds.
func (c *Client) Acquire(name string, opts ...Option) (*Lock, error) {
	l := c.newLock(name, opts)
	for {
		err := c.tryAcquire(l)
		if l.failIfLocked && err == ErrNotAcquired {
			c.log.Println("not acquired, exit")
			return l, err
		} else if err == ErrNotAcquired {
			c.log.Println("not acquired, wait:", l.leaseDuration)
			time.Sleep(l.leaseDuration)
			continue
		} else if errors.Is(errors.FailedPrecondition, err) {
			c.log.Println("bad transaction, retrying:", err)
			continue
		} else if err != nil {
			c.log.Println("error:", err)
			return nil, err
		}
		return l, nil
	}
}

func (c *Client) tryAcquire(l *Lock) error {
	err := c.storeAcquire(l)
	if err == nil {
		ctx, cancel := context.WithCancel(context.Background())
		l.heartbeatCancel = cancel
		go c.heartbeat(ctx, l)
	}
	return err
}

func (c *Client) storeAcquire(l *Lock) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return errors.E(err, "cannot create transaction for lock acquisition")
	}
	rvn := randString(32)
	c.log.Println("storeAcquire in", l.name, rvn, l.data, l.recordVersionNumber)
	defer func() {
		c.log.Println("storeAcquire out", l.name, rvn, l.data, l.recordVersionNumber)
	}()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO `+c.tableName+`
			("name", "record_version_number", "data")
		VALUES
			($1, $2, $3)
		ON CONFLICT ("name") DO UPDATE
		SET
			"record_version_number" = $2,
			"data" = CASE
				WHEN $5 THEN $3
				ELSE `+c.tableName+`."data"
			END
		WHERE
			`+c.tableName+`."record_version_number" IS NULL
			OR `+c.tableName+`."record_version_number" = $4
	`, l.name, rvn, l.data, l.recordVersionNumber, l.replaceData)
	if err != nil {
		spew.Dump(c.tableName, err)
		return errors.E(errors.FailedPrecondition, err, "cannot run query to acquire lock")
	}
	row := tx.QueryRowContext(ctx, `SELECT "record_version_number", "data" FROM `+c.tableName+` WHERE name = $1 FOR UPDATE`, l.name)
	var actualRVN string
	var data []byte
	if err := row.Scan(&actualRVN, &data); err == sql.ErrNoRows {
		return ErrLockNotFound
	} else if err != nil {
		return errors.E(errors.FailedPrecondition, err, "cannot load information for lock acquisition")
	}
	if actualRVN != rvn {
		l.recordVersionNumber = actualRVN
		return ErrNotAcquired
	}
	if err := tx.Commit(); err != nil {
		return errors.E(errors.FailedPrecondition, err, "cannot commit lock acquisition")
	}
	l.recordVersionNumber = rvn
	l.data = data
	return nil
}

// Release will update the mutex entry to be able to be taken by other clients.
func (c *Client) Release(l *Lock) error {
	if l.IsReleased() {
		return ErrLockAlreadyReleased
	}
	for {
		err := c.storeRelease(l)
		if errors.Is(errors.FailedPrecondition, err) {
			c.log.Println("cannot release lock, trying again:", err)
			continue
		}
		return err
	}
}

func (c *Client) storeRelease(l *Lock) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return errors.E(err, "cannot create transaction for lock acquisition")
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE
			`+c.tableName+`
		SET
			"record_version_number" = NULL
		WHERE
			"name" = $1
			AND "record_version_number" = $2
	`, l.name, l.recordVersionNumber)
	if err != nil {
		return errors.E(errors.FailedPrecondition, err, "cannot run query to release lock")
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return errors.E(errors.FailedPrecondition, err, "cannot confirm whether the lock has been released")
	} else if affected == 0 {
		l.isReleased = true
		l.heartbeatCancel()
		return ErrLockAlreadyReleased
	}
	if l.deleteOnRelease {
		_, err := tx.ExecContext(ctx, `
		DELETE FROM
			`+c.tableName+`
		WHERE
			"name" = $1
			AND "record_version_number" IS NULL`, l.name)
		if err != nil {
			return errors.E(errors.FailedPrecondition, err, "cannot run query to delete lock")
		}
	}
	if err := tx.Commit(); err != nil {
		return errors.E(errors.FailedPrecondition, err, "cannot commit lock release")
	}
	l.isReleased = true
	l.heartbeatCancel()
	return nil
}

func (c *Client) heartbeat(ctx context.Context, l *Lock) {
	defer c.log.Println("heartbeat stopped:", l.name)
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(c.heartbeatFrequency):
			for {
				err := c.storeHeartbeat(l)
				if errors.Is(errors.FailedPrecondition, err) {
					c.log.Println("heartbeat retry:", l.name, err)
					continue
				} else if err != nil {
					c.log.Println("heartbeat missed:", l.name, err)
					return
				}
				break
			}
		}
	}
}

func (c *Client) storeHeartbeat(l *Lock) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return errors.E(err, "cannot create transaction for lock acquisition")
	}
	rvn := randString(32)
	result, err := tx.ExecContext(ctx, `
		UPDATE
			`+c.tableName+`
		SET
			"record_version_number" = $3
		WHERE
			"name" = $1
			AND "record_version_number" = $2
	`, l.name, l.recordVersionNumber, rvn)
	if err != nil {
		return errors.E(errors.FailedPrecondition, err, "cannot run query to update the heartbeat")
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return errors.E(errors.FailedPrecondition, err, "cannot confirm whether the lock has been updated for the heartbeat")
	} else if affected == 0 {
		l.isReleased = true
		return ErrLockAlreadyReleased
	}
	if err := tx.Commit(); err != nil {
		return errors.E(errors.FailedPrecondition, err, "cannot commit lock heartbeat")
	}
	l.recordVersionNumber = rvn
	return nil
}

// GetData returns the data field from the given lock in the table without holding
// the lock first
func (c *Client) GetData(name string) ([]byte, error) {
	for {
		data, err := c.getLock(name)
		if errors.Is(errors.FailedPrecondition, err) {
			c.log.Println("cannot get lock entry:", err)
			continue
		} else if errors.Is(errors.NotExist, err) {
			c.log.Println("missing lock entry:", err)
			return data, err
		}
		return data, err
	}
}

func (c *Client) getLock(name string) ([]byte, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	row := c.db.QueryRowContext(ctx, `
		SELECT
			"data"
		FROM
			`+c.tableName+`
		WHERE
			"name" = $1
		FOR UPDATE
	`, name)
	var data []byte
	err := row.Scan(&data)
	if err == sql.ErrNoRows {
		return data, ErrLockNotFound
	}
	return data, errors.E(errors.FailedPrecondition, err, "cannot load the data of this lock")
}

// ClientOption reconfigures the lock client
type ClientOption func(*Client)

// WithLogger injects a logger into the client, so its internals can be
// recorded.
func WithLogger(l Logger) ClientOption {
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
