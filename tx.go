package database

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
	"github.com/opentracing/opentracing-go"
)

func (db *DB) RunTxx(ctx context.Context, fn TransactionFunc) error {
	if parent := opentracing.SpanFromContext(ctx); parent != nil {
		span := parent.Tracer().StartSpan(transactionSpanName, opentracing.ChildOf(parent.Context()))
		defer span.Finish()

		ctx = opentracing.ContextWithSpan(ctx, span)
	}

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
