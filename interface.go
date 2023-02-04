package database

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

//nolint:interfacebloat // it's ok
type Database interface {
	// SelectContext using this DB.
	// Any placeholder parameters are replaced with supplied args.
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error

	// GetContext using this DB.
	// Any placeholder parameters are replaced with supplied args.
	// An error is returned if the result set is empty.
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error

	// BindNamed binds a query using the DB driver's bindvar type.
	BindNamed(query string, arg interface{}) (bound string, arglist []interface{}, err error)

	// BeginTxx begins a transaction and returns an *sqlx.Tx instead of an
	// *sql.Tx.
	//
	// The provided context is used until the transaction is committed or rolled
	// back. If the context is canceled, the sql package will roll back the
	// transaction. Tx.Commit will return an error if the context provided to
	// BeginxContext is canceled.
	BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error)

	// ExecContext executes a query without returning any rows.
	// The args are for any placeholder parameters in the query.
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)

	// NamedExecContext using this DB.
	// Any named placeholder parameters are replaced with fields from arg.
	NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error)

	// QueryxContext queries the database and returns an *sqlx.Rows.
	// Any placeholder parameters are replaced with supplied args.
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)

	// NamedQueryContext using this DB.
	// Any named placeholder parameters are replaced with fields from arg.
	NamedQueryContext(ctx context.Context, query string, arg interface{}) (*sqlx.Rows, error)

	// PreparexContext returns an sqlx.Stmt instead of a sqlx.Stmt.
	PreparexContext(ctx context.Context, query string) (*sqlx.Stmt, error)

	// PrepareNamedContext returns an sqlx.NamedStmt.
	PrepareNamedContext(ctx context.Context, query string) (*sqlx.NamedStmt, error)

	// RunTxx runs transaction callback func with default `sql.TxOptions`.
	// If an error occurs, the transaction will be retried if it allows `RetryFunc`.
	RunTxx(ctx context.Context, fn TransactionFunc) error

	// RunReadTxx runs transaction callback func with read only `sql.TxOptions`.
	// If an error occurs, the transaction will be retried if it allows `RetryFunc`.
	RunReadTxx(ctx context.Context, fn TransactionFunc) error

	// RunTxxWithOptions runs transaction callback func with custom `sql.TxOptions`.
	// If an error occurs, the transaction will be retried if it allows `RetryFunc`.
	RunTxxWithOptions(ctx context.Context, opts *sql.TxOptions, fn TransactionFunc) error

	// Close closes the database and prevents new queries from starting.
	// Close then waits for all queries that have started processing on the server
	// to finish.
	//
	// It is rare to Close a DB, as the DB handle is meant to be
	// long-lived and shared between many goroutines.
	Close() error
}
