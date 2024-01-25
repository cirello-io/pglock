package pglock

import (
	"context"
	"database/sql"
)

type sqlLike interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (txLike, error)
	Exec(sql string, arguments ...any) (any, error)
	QueryContext(ctx context.Context, query string, args ...any) (rowsLike, error)
	QueryRowContext(ctx context.Context, query string, args ...any) rowLike
	Close() error
}

type txLike interface {
	ExecContext(ctx context.Context, sql string, arguments ...any) (resultLike, error)
	QueryRowContext(ctx context.Context, query string, args ...any) rowLike
	Commit() error
}

type rowLike interface {
	Scan(dest ...any) error
}

type rowsLike interface {
	rowLike
	Close() error
	Next() bool
	Err() error
}

type resultLike interface {
	RowsAffected() (int64, error)
}
