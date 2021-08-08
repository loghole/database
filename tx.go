package database

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
	"github.com/opentracing/opentracing-go"
)

func (d *DB) RunTxx(ctx context.Context, fn TransactionFunc) error {
	if parent := opentracing.SpanFromContext(ctx); parent != nil {
		span := parent.Tracer().StartSpan(transactionSpanName, opentracing.ChildOf(parent.Context()))
		defer span.Finish()

		ctx = opentracing.ContextWithSpan(ctx, span)
	}

	tx, err := d.BeginTxx(ctx, &sql.TxOptions{})
	if err != nil {
		return err //nolint:wrapcheck // need clear error
	}

	defer d.rollback(tx)

	var retryCount int

	// Retry transaction for cockroach db.
	for {
		if err = fn(ctx, tx); !d.errIsRetryable(retryCount, err) {
			break
		}

		retryCount++
	}

	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err // nolint:wrapcheck // need clean error
	}

	return nil
}

func (d *DB) rollback(tx *sqlx.Tx) {
	_ = tx.Rollback()
}

func (d *DB) errIsRetryable(retryCount int, err error) bool {
	if d.retryFunc != nil {
		return d.retryFunc(retryCount, err)
	}

	return false
}
