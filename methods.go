package database

import (
	"context"
	"database/sql"
	"math"
	"math/rand"
	"time"

	"github.com/jmoiron/sqlx"
)

// SelectContext using this DB.
// Any placeholder parameters are replaced with supplied args.
func (db *DB) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return db.withRetry(ctx, func() error {
		return db.DB.SelectContext(ctx, dest, query, args...)
	})
}

// GetContext using this DB.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (db *DB) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return db.withRetry(ctx, func() error {
		return db.DB.GetContext(ctx, dest, query, args...)
	})
}

// BindNamed binds a query using the DB driver's bindvar type.
func (db *DB) BindNamed(query string, arg interface{}) (bound string, arglist []interface{}, err error) {
	err = db.withRetry(context.Background(), func() error {
		var err error

		if bound, arglist, err = db.DB.BindNamed(query, arg); err != nil {
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
	err = db.withRetry(ctx, func() error {
		var err error

		if tx, err = db.DB.BeginTxx(ctx, opts); err != nil {
			return err
		}

		return nil
	})

	return tx, err
}

// ExecContext executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (result sql.Result, err error) {
	err = db.withRetry(ctx, func() error {
		var err error

		if result, err = db.DB.ExecContext(ctx, query, args...); err != nil {
			return err
		}

		return nil
	})

	return result, err
}

// NamedExecContext using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *DB) NamedExecContext(ctx context.Context, query string, arg interface{}) (result sql.Result, err error) {
	err = db.withRetry(ctx, func() error {
		var err error

		if result, err = db.DB.NamedExecContext(ctx, query, arg); err != nil {
			return err
		}

		return nil
	})

	return result, err
}

// QueryxContext queries the database and returns an *sqlx.Rows.
// Any placeholder parameters are replaced with supplied args.
func (db *DB) QueryxContext(ctx context.Context, query string, args ...interface{}) (rows *sqlx.Rows, err error) {
	err = db.withRetry(ctx, func() error {
		var err error

		// nolint:sqlclosecheck // will be closed at the upper level
		if rows, err = db.DB.QueryxContext(ctx, query, args...); err != nil {
			return err
		}

		return nil
	})

	return rows, err
}

// NamedQueryContext using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *DB) NamedQueryContext(ctx context.Context, query string, arg interface{}) (rows *sqlx.Rows, err error) {
	err = db.withRetry(ctx, func() error {
		var err error

		// nolint:sqlclosecheck // will be closed at the upper level
		if rows, err = db.DB.NamedQueryContext(ctx, query, arg); err != nil {
			return err
		}

		return nil
	})

	return rows, err
}

// PreparexContext returns an sqlx.Stmt instead of a sqlx.Stmt.
func (db *DB) PreparexContext(ctx context.Context, query string) (stmt *sqlx.Stmt, err error) {
	err = db.withRetry(ctx, func() error {
		var err error

		// nolint:sqlclosecheck // will be closed at the upper level
		if stmt, err = db.DB.PreparexContext(ctx, query); err != nil {
			return err
		}

		return nil
	})

	return stmt, err
}

// PrepareNamedContext returns an sqlx.NamedStmt.
func (db *DB) PrepareNamedContext(ctx context.Context, query string) (stmt *sqlx.NamedStmt, err error) {
	err = db.withRetry(ctx, func() error {
		var err error

		if stmt, err = db.DB.PrepareNamedContext(ctx, query); err != nil {
			return err
		}

		return nil
	})

	return stmt, err
}

func (db *DB) withRetry(ctx context.Context, fn func() error) error {
	if db.options.retryPolicy == nil {
		return fn()
	}

	var (
		retryPolicy = db.options.retryPolicy

		attempt int
		err     error
	)

	for {
		attempt++

		if err = fn(); err == nil || !retryPolicy.ErrIsRetryable(err) {
			return err
		}

		if attempt >= retryPolicy.MaxAttempts {
			return ErrMaxRetryAttempts
		}

		var (
			fact = math.Pow(retryPolicy.BackoffMultiplier, float64(attempt))
			cur  = float64(retryPolicy.InitialBackoff) * fact
		)

		if max := float64(retryPolicy.MaxBackoff); cur > max {
			cur = max
		}

		timer := time.NewTimer(time.Duration(rand.Int63n(int64(cur)))) // nolint:gosec // normal for this case.
		select {
		case <-timer.C:
			continue
		case <-ctx.Done():
			timer.Stop()

			return ctx.Err()
		}
	}
}
