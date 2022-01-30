package database

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

// SelectContext using this DB.
// Any placeholder parameters are replaced with supplied args.
func (db *DB) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return db.runQuery(ctx, func(ctx context.Context, db *sqlx.DB) error {
		return db.SelectContext(ctx, dest, query, args...)
	})
}

// GetContext using this DB.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (db *DB) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return db.runQuery(ctx, func(ctx context.Context, db *sqlx.DB) error {
		return db.GetContext(ctx, dest, query, args...)
	})
}

// BindNamed binds a query using the DB driver's bindvar type.
func (db *DB) BindNamed(query string, arg interface{}) (bound string, arglist []interface{}, err error) {
	err = db.runQuery(context.Background(), func(ctx context.Context, db *sqlx.DB) error {
		var err error

		if bound, arglist, err = db.BindNamed(query, arg); err != nil {
			return err
		}

		return nil
	})

	return bound, arglist, err
}

// BeginTxx begins a transaction and returns an *sqlx.Tx instead of an
// *sql.Tx.
//
// The provided context is used until the transaction is committed or rolled
// back. If the context is canceled, the sql package will roll back the
// transaction. Tx.Commit will return an error if the context provided to
// BeginxContext is canceled.
func (db *DB) BeginTxx(ctx context.Context, opts *sql.TxOptions) (tx *sqlx.Tx, err error) {
	err = db.runQuery(ctx, func(ctx context.Context, db *sqlx.DB) error {
		var err error

		if tx, err = db.BeginTxx(ctx, opts); err != nil {
			return err
		}

		return nil
	})

	return tx, err
}

// ExecContext executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (result sql.Result, err error) {
	err = db.runQuery(ctx, func(ctx context.Context, db *sqlx.DB) error {
		var err error

		if result, err = db.ExecContext(ctx, query, args...); err != nil {
			return err
		}

		return nil
	})

	return result, err
}

// NamedExecContext using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *DB) NamedExecContext(ctx context.Context, query string, arg interface{}) (result sql.Result, err error) {
	err = db.runQuery(ctx, func(ctx context.Context, db *sqlx.DB) error {
		var err error

		if result, err = db.NamedExecContext(ctx, query, arg); err != nil {
			return err
		}

		return nil
	})

	return result, err
}

// QueryxContext queries the database and returns an *sqlx.Rows.
// Any placeholder parameters are replaced with supplied args.
func (db *DB) QueryxContext(ctx context.Context, query string, args ...interface{}) (rows *sqlx.Rows, err error) {
	err = db.runQuery(ctx, func(ctx context.Context, db *sqlx.DB) error {
		var err error

		// nolint:sqlclosecheck // will be closed at the upper level
		if rows, err = db.QueryxContext(ctx, query, args...); err != nil {
			return err
		}

		return nil
	})

	return rows, err
}

// NamedQueryContext using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *DB) NamedQueryContext(ctx context.Context, query string, arg interface{}) (rows *sqlx.Rows, err error) {
	err = db.runQuery(ctx, func(ctx context.Context, db *sqlx.DB) error {
		var err error

		// nolint:sqlclosecheck // will be closed at the upper level
		if rows, err = db.NamedQueryContext(ctx, query, arg); err != nil {
			return err
		}

		return nil
	})

	return rows, err
}

// PreparexContext returns an sqlx.Stmt instead of a sqlx.Stmt.
func (db *DB) PreparexContext(ctx context.Context, query string) (stmt *sqlx.Stmt, err error) {
	err = db.runQuery(ctx, func(ctx context.Context, db *sqlx.DB) error {
		var err error

		// nolint:sqlclosecheck // will be closed at the upper level
		if stmt, err = db.PreparexContext(ctx, query); err != nil {
			return err
		}

		return nil
	})

	return stmt, err
}

// PrepareNamedContext returns an sqlx.NamedStmt.
func (db *DB) PrepareNamedContext(ctx context.Context, query string) (stmt *sqlx.NamedStmt, err error) {
	err = db.runQuery(ctx, func(ctx context.Context, db *sqlx.DB) error {
		var err error

		if stmt, err = db.PrepareNamedContext(ctx, query); err != nil {
			return err
		}

		return nil
	})

	return stmt, err
}

func (db *DB) runQuery(ctx context.Context, fn QueryFunc) (err error) {
	var retryCount int

	for {
		retryCount++

		if err = fn(ctx, db.DB); !db.errIsRetryable(retryCount, err) {
			break
		}
	}

	if err != nil {
		return err
	}

	return nil
}
