package database

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
	"go.opentelemetry.io/otel/trace"
)

// RunTxx runs transaction callback func with default `sql.TxOptions`.
// If an error occurs, the transaction will be retried if it allows `RetryFunc`.
//
// Example:
//
//	err := db.RunTxx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
//		var val time.Time
//
//		if err := tx.GetContext(ctx, &val, "SELECT now()"); err != nil {
//			return err
//		}
//
//		return nil
//	})
//	if err != nil {
//		return err
//	}
func (db *DB) RunTxx(ctx context.Context, fn TransactionFunc) error {
	return db.RunTxxWithOptions(ctx, &sql.TxOptions{}, fn)
}

// RunReadTxx runs transaction callback func with read only `sql.TxOptions`.
// If an error occurs, the transaction will be retried if it allows `RetryFunc`.
func (db *DB) RunReadTxx(ctx context.Context, fn TransactionFunc) error {
	return db.RunTxxWithOptions(ctx, &sql.TxOptions{ReadOnly: true}, fn)
}

// RunTxxWithOptions runs transaction callback func with custom `sql.TxOptions`.
// If an error occurs, the transaction will be retried if it allows `RetryFunc`.
func (db *DB) RunTxxWithOptions(ctx context.Context, opts *sql.TxOptions, fn TransactionFunc) error {
	ctx, span := trace.
		SpanFromContext(ctx).
		TracerProvider().
		Tracer(_defaultTracerName).
		Start(ctx, _txSpanName, trace.WithSpanKind(trace.SpanKindInternal))
	defer span.End()

	return db.withRetry(ctx, func() error { return db.runTxx(ctx, opts, fn) })
}

func (db *DB) runTxx(ctx context.Context, opts *sql.TxOptions, fn TransactionFunc) error {
	tx, err := db.DB.BeginTxx(ctx, opts)
	if err != nil {
		return err
	}

	defer db.rollback(tx)

	if err := fn(ctx, tx); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err //nolint:wrapcheck // need clean error
	}

	return nil
}

func (db *DB) rollback(tx *sqlx.Tx) {
	_ = tx.Rollback()
}
