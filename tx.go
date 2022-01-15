package database

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
	"go.opentelemetry.io/otel/trace"
)

func (db *DB) RunTxx(ctx context.Context, fn TransactionFunc) error {
	ctx, span := trace.
		SpanFromContext(ctx).
		TracerProvider().
		Tracer(_tracerName).
		Start(ctx, _transactionSpanName)
	defer span.End()

	var (
		retryCount int
		err        error
	)

	// Retry transaction.
	for {
		if err = db.runTxx(ctx, fn); !db.errIsRetryable(retryCount, err) {
			break
		}

		retryCount++
	}

	if err != nil {
		return err
	}

	return nil
}

func (db *DB) runTxx(ctx context.Context, fn TransactionFunc) error {
	tx, err := db.BeginTxx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}

	defer db.rollback(tx)

	if err := fn(ctx, tx); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err // nolint:wrapcheck // need clean error
	}

	return nil
}

func (db *DB) rollback(tx *sqlx.Tx) {
	_ = tx.Rollback()
}

func (db *DB) errIsRetryable(retryCount int, err error) bool {
	if db.retryFunc != nil {
		return db.retryFunc(retryCount, err)
	}

	return false
}
