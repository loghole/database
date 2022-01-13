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
	if err := db.runQuery(context.Background(), func(ctx context.Context, db *sqlx.DB) error {
		var err error

		if bound, arglist, err = db.BindNamed(query, arg); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return "", nil, err
	}

	return bound, arglist, nil
}

// Beginx begins a transaction and returns an *sqlx.Tx instead of an *sql.Tx.
func (db *DB) Beginx() (*sqlx.Tx, error) {
	var tx *sqlx.Tx

	if err := db.runQuery(context.Background(), func(ctx context.Context, db *sqlx.DB) error {
		var err error

		if tx, err = db.Beginx(); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return tx, nil
}

// BeginTxx begins a transaction and returns an *sqlx.Tx instead of an
// *sql.Tx.
//
// The provided context is used until the transaction is committed or rolled
// back. If the context is canceled, the sql package will roll back the
// transaction. Tx.Commit will return an error if the context provided to
// BeginxContext is canceled.
func (db *DB) BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error) {
	var tx *sqlx.Tx

	if err := db.runQuery(ctx, func(ctx context.Context, db *sqlx.DB) error {
		var err error

		if tx, err = db.BeginTxx(ctx, opts); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return tx, nil
}

// ExecContext executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	var result sql.Result

	if err := db.runQuery(ctx, func(ctx context.Context, db *sqlx.DB) error {
		var err error

		if result, err = db.ExecContext(ctx, query, args...); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return result, nil
}

// NamedExecContext using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *DB) NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	var result sql.Result

	if err := db.runQuery(ctx, func(ctx context.Context, db *sqlx.DB) error {
		var err error

		if result, err = db.NamedExecContext(ctx, query, arg); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return result, nil
}

// QueryxContext queries the database and returns an *sqlx.Rows.
// Any placeholder parameters are replaced with supplied args.
func (db *DB) QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	var rows *sqlx.Rows

	if err := db.runQuery(ctx, func(ctx context.Context, db *sqlx.DB) error {
		var err error

		// nolint:sqlclosecheck // will be closed at the upper level
		if rows, err = db.QueryxContext(ctx, query, args...); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return rows, nil
}

// NamedQueryContext using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *DB) NamedQueryContext(ctx context.Context, query string, arg interface{}) (*sqlx.Rows, error) {
	var rows *sqlx.Rows

	if err := db.runQuery(ctx, func(ctx context.Context, db *sqlx.DB) error {
		var err error

		// nolint:sqlclosecheck // will be closed at the upper level
		if rows, err = db.NamedQueryContext(ctx, query, arg); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return rows, nil
}

// PreparexContext returns an sqlx.Stmt instead of a sqlx.Stmt.
func (db *DB) PreparexContext(ctx context.Context, query string) (*sqlx.Stmt, error) {
	var stmt *sqlx.Stmt

	if err := db.runQuery(ctx, func(ctx context.Context, db *sqlx.DB) error {
		var err error

		// nolint:sqlclosecheck // will be closed at the upper level
		if stmt, err = db.PreparexContext(ctx, query); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return stmt, nil
}

// PrepareNamedContext returns an sqlx.NamedStmt.
func (db *DB) PrepareNamedContext(ctx context.Context, query string) (*sqlx.NamedStmt, error) {
	var stmt *sqlx.NamedStmt

	if err := db.runQuery(ctx, func(ctx context.Context, db *sqlx.DB) error {
		var err error

		if stmt, err = db.PrepareNamedContext(ctx, query); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (db *DB) runQuery(ctx context.Context, cb QueryFunc) (err error) {
	var retryCount int

	for {
		if err = cb(ctx, db.DB); !db.errIsRetryable(retryCount, err) {
			break
		}

		retryCount++
	}

	if err != nil {
		return err
	}

	return nil
}
