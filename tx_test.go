package database

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/trace"
)

func TestDB_RunTxx(t *testing.T) {
	type args struct {
		ctx context.Context
		fn  TransactionFunc
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "pass",
			args: args{
				ctx: context.Background(),
				fn: func(ctx context.Context, tx *sqlx.Tx) error {
					if _, err := tx.ExecContext(ctx, "CREATE TABLE foo (id INTEGER)"); err != nil {
						return err
					}

					if _, err := tx.ExecContext(ctx, "INSERT INTO foo (id) VALUES (1)"); err != nil {
						return err
					}

					if _, err := tx.ExecContext(ctx, "INSERT INTO foo (id) VALUES (2)"); err != nil {
						return err
					}

					var result []int64

					if err := tx.SelectContext(ctx, &result, "SELECT id FROM foo"); err != nil {
						return err
					}

					assert.Equal(t, result, []int64{1, 2})

					return nil
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "invalid query error",
			args: args{
				ctx: context.Background(),
				fn: func(ctx context.Context, tx *sqlx.Tx) error {
					if _, err := tx.ExecContext(ctx, "bad_query"); err != nil {
						return err
					}

					return nil
				},
			},
			wantErr: assert.Error,
		},
		{
			name: "commit error",
			args: args{
				ctx: context.Background(),
				fn: func(ctx context.Context, tx *sqlx.Tx) error {
					if err := tx.Rollback(); err != nil {
						return err
					}

					return nil
				},
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := memorySQLLite(t, WithDefaultOptions(trace.NewNoopTracerProvider().Tracer("")))
			defer db.Close()

			tt.wantErr(t, db.RunTxx(tt.args.ctx, tt.args.fn), fmt.Sprintf("RunTxx(%v, %v)", tt.args.ctx, tt.args.fn))
		})
	}
}

func TestDB_RunReadTxx(t *testing.T) {
	type args struct {
		ctx context.Context
		fn  TransactionFunc
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "pass",
			args: args{
				ctx: context.Background(),
				fn: func(ctx context.Context, tx *sqlx.Tx) error {
					var result []int64

					if err := tx.SelectContext(ctx, &result, "WITH q(number) as (values (1),(2),(3),(4),(5)) SELECT * from q"); err != nil {
						return err
					}

					assert.Equal(t, result, []int64{1, 2, 3, 4, 5})

					return nil
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "invalid query error",
			args: args{
				ctx: context.Background(),
				fn: func(ctx context.Context, tx *sqlx.Tx) error {
					if _, err := tx.ExecContext(ctx, "bad_query"); err != nil {
						return err
					}

					return nil
				},
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := memorySQLLite(t, WithDefaultOptions(trace.NewNoopTracerProvider().Tracer("")))
			defer db.Close()

			tt.wantErr(t, db.RunReadTxx(tt.args.ctx, tt.args.fn), fmt.Sprintf("RunReadTxx(%v, %v)", tt.args.ctx, tt.args.fn))
		})
	}
}

func TestDB_RunTxxWithOptions(t *testing.T) {
	type args struct {
		ctx  context.Context
		opts *sql.TxOptions
		fn   TransactionFunc
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "pass",
			args: args{
				ctx:  context.Background(),
				opts: &sql.TxOptions{},
				fn: func(ctx context.Context, tx *sqlx.Tx) error {
					var result []int64

					if err := tx.SelectContext(ctx, &result, "WITH q(number) as (values (1),(2),(3),(4),(5)) SELECT * from q"); err != nil {
						return err
					}

					assert.Equal(t, result, []int64{1, 2, 3, 4, 5})

					return nil
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "pass with nil options",
			args: args{
				ctx:  context.Background(),
				opts: nil,
				fn: func(ctx context.Context, tx *sqlx.Tx) error {
					var result []int64

					if err := tx.SelectContext(ctx, &result, "WITH q(number) as (values (1),(2),(3),(4),(5)) SELECT * from q"); err != nil {
						return err
					}

					assert.Equal(t, result, []int64{1, 2, 3, 4, 5})

					return nil
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "invalid query error",
			args: args{
				ctx: context.Background(),
				fn: func(ctx context.Context, tx *sqlx.Tx) error {
					if _, err := tx.ExecContext(ctx, "bad_query"); err != nil {
						return err
					}

					return nil
				},
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := memorySQLLite(t, WithDefaultOptions(trace.NewNoopTracerProvider().Tracer("")))
			defer db.Close()

			tt.wantErr(t, db.RunTxxWithOptions(tt.args.ctx, tt.args.opts, tt.args.fn), fmt.Sprintf("RunTxxWithOptions(%v, %v, %v)", tt.args.ctx, tt.args.opts, tt.args.fn))
		})
	}
}

func TestDB_RetryTx(t *testing.T) {
	var errors []string

	db := memorySQLLite(t, WithRetryPolicy(RetryPolicy{
		MaxAttempts:       5,
		InitialBackoff:    1,
		MaxBackoff:        1,
		BackoffMultiplier: 1,
		ErrIsRetryable: func(err error) bool {
			errors = append(errors, err.Error())

			return true
		},
	}))

	err := db.RunTxx(context.Background(), func(ctx context.Context, tx *sqlx.Tx) error {
		_, err := tx.ExecContext(context.Background(), "SELECT * FROM foo")
		return err
	})

	assert.Error(t, err, "db.RunTxx")
	assert.Equal(t, errors, []string{
		"no such table: foo",
		"no such table: foo",
		"no such table: foo",
		"no such table: foo",
		"no such table: foo",
	})
}
