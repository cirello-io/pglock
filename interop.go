package pglock

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

func FromSQLDB(db *sql.DB) *sqldbWrapper {
	return &sqldbWrapper{
		DB: db,
	}
}

var (
	_ sqlLike = (*sqldbWrapper)(nil)
	_ txLike  = (*sqlTxWrapper)(nil)
)

type sqldbWrapper struct {
	*sql.DB
}

func (s *sqldbWrapper) BeginTx(ctx context.Context, opts *sql.TxOptions) (txLike, error) {
	t, err := s.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &sqlTxWrapper{Tx: t}, nil
}

func (s *sqldbWrapper) Exec(sql string, arguments ...any) (any, error) {
	return s.DB.Exec(sql, arguments...)
}

func (s *sqldbWrapper) QueryRowContext(ctx context.Context, query string, args ...any) rowLike {
	return s.DB.QueryRowContext(ctx, query, args...)
}

func (s *sqldbWrapper) QueryContext(ctx context.Context, query string, args ...any) (rowsLike, error) {
	return s.DB.QueryContext(ctx, query, args...)
}

type sqlTxWrapper struct {
	*sql.Tx
}

func (t *sqlTxWrapper) ExecContext(ctx context.Context, sql string, arguments ...any) (resultLike, error) {
	return t.Tx.ExecContext(ctx, sql, arguments...)
}

func (t *sqlTxWrapper) QueryRowContext(ctx context.Context, query string, args ...any) rowLike {
	return t.Tx.QueryRowContext(ctx, query, args...)
}

func FromPGXPool(db *pgxpool.Pool) *pgxpoolWrapper {
	return &pgxpoolWrapper{
		Pool: db,
	}
}

var (
	_ sqlLike    = (*pgxpoolWrapper)(nil)
	_ txLike     = (*pgxTxWrapper)(nil)
	_ resultLike = (*pgxResultWrapper)(nil)
)

type pgxpoolWrapper struct {
	*pgxpool.Pool
}

func (s *pgxpoolWrapper) BeginTx(ctx context.Context, opts *sql.TxOptions) (txLike, error) {
	if opts.Isolation != sql.LevelSerializable {
		return nil, errors.New("pglock: pgxpoolWrapper only supports serializable isolation")
	}

	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.Serializable,
	})
	if err != nil {
		return nil, err
	}
	return &pgxTxWrapper{Tx: tx}, nil
}

func (s *pgxpoolWrapper) Exec(sql string, arguments ...any) (any, error) {
	return s.Pool.Exec(context.Background(), sql, arguments...)
}

func (s *pgxpoolWrapper) QueryContext(ctx context.Context, query string, args ...any) (rowsLike, error) {
	r, err := s.Pool.Query(ctx, query, args...)
	return &pgxRowsWrapper{Rows: r}, err
}

func (s *pgxpoolWrapper) QueryRowContext(ctx context.Context, query string, args ...any) rowLike {
	return s.Pool.QueryRow(ctx, query, args...)
}

func (s *pgxpoolWrapper) Close() error {
	return nil
}

type pgxTxWrapper struct {
	pgx.Tx
}

func (t *pgxTxWrapper) ExecContext(ctx context.Context, sql string, arguments ...any) (resultLike, error) {
	ct, err := t.Tx.Exec(ctx, sql, arguments...)
	return &pgxResultWrapper{CommandTag: &ct}, err
}

func (t *pgxTxWrapper) QueryRowContext(ctx context.Context, query string, args ...any) rowLike {
	return t.Tx.QueryRow(ctx, query, args...)
}

func (t *pgxTxWrapper) Commit() error {
	return t.Tx.Commit(context.Background())
}

type pgxRowsWrapper struct {
	pgx.Rows
}

func (p *pgxRowsWrapper) Close() error {
	p.Rows.Close()
	return nil
}

type pgxResultWrapper struct {
	*pgconn.CommandTag
}

func (r *pgxResultWrapper) RowsAffected() (int64, error) {
	return r.CommandTag.RowsAffected(), nil
}
