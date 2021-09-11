package database

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

// BindNamed binds a query using the DB driver's bindvar type.
func (db *DB) BindNamed(query string, arg interface{}) (string, []interface{}, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.db.BindNamed(query, arg)
}

// Beginx begins a transaction and returns an *sqlx.Tx instead of an *sql.Tx.
func (db *DB) Beginx() (*sqlx.Tx, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.db.Beginx()
}

// BeginTxx begins a transaction and returns an *sqlx.Tx instead of an
// *sql.Tx.
//
// The provided context is used until the transaction is committed or rolled
// back. If the context is canceled, the sql package will roll back the
// transaction. Tx.Commit will return an error if the context provided to
// BeginxContext is canceled.
func (db *DB) BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.db.BeginTxx(ctx, opts)
}

// GetContext using this DB.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (db *DB) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.db.GetContext(ctx, dest, query, args...)
}

// SelectContext using this DB.
// Any placeholder parameters are replaced with supplied args.
func (db *DB) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.db.SelectContext(ctx, dest, query, args...)
}

// ExecContext executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.db.ExecContext(ctx, query, args...)
}

// NamedExecContext using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *DB) NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.db.NamedExecContext(ctx, query, arg)
}

// QueryxContext queries the database and returns an *sqlx.Rows.
// Any placeholder parameters are replaced with supplied args.
func (db *DB) QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.db.QueryxContext(ctx, query, args...)
}

// NamedQueryContext using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *DB) NamedQueryContext(ctx context.Context, query string, arg interface{}) (*sqlx.Rows, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.db.NamedQueryContext(ctx, query, arg)
}

// PreparexContext returns an sqlx.Stmt instead of a sqlx.Stmt.
func (db *DB) PreparexContext(ctx context.Context, query string) (*sqlx.Stmt, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.db.PreparexContext(ctx, query)
}

// PrepareNamedContext returns an sqlx.NamedStmt.
func (db *DB) PrepareNamedContext(ctx context.Context, query string) (*sqlx.NamedStmt, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.db.PrepareNamedContext(ctx, query)
}
