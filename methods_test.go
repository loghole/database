package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

func TestDB_SelectContext(t *testing.T) {
	type args struct {
		ctx   context.Context
		query string
		args  []interface{}
	}
	tests := []struct {
		name    string
		prepare func(db *DB)
		args    args
		want    []interface{}
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "pass",
			args: args{
				ctx:   context.Background(),
				query: "WITH q(number) as (values (1),(2),(3),(4),(5)) SELECT * from q",
				args:  nil,
			},
			want:    []interface{}{int64(1), int64(2), int64(3), int64(4), int64(5)},
			wantErr: assert.NoError,
		},
		{
			name: "invalid query",
			args: args{
				ctx:   context.Background(),
				query: "SELEC",
				args:  nil,
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := memorySQLLite(t, WithDefaultOptions(trace.NewNoopTracerProvider().Tracer("")))
			defer db.Close()

			var dest []interface{}

			err := db.SelectContext(tt.args.ctx, &dest, tt.args.query, tt.args.args...)
			if !tt.wantErr(t, err, "db.SelectContext()") {
				return
			}

			if err == nil {
				assert.Equal(t, tt.want, dest)
			}
		})
	}
}

func TestDB_GetContext(t *testing.T) {
	type args struct {
		ctx   context.Context
		dest  interface{}
		query string
		args  []interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    interface{}
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "pass",
			args: args{
				ctx:   context.Background(),
				query: "SELECT 'qwerty'",
				args:  nil,
			},
			want:    "qwerty",
			wantErr: assert.NoError,
		},
		{
			name: "invalid query",
			args: args{
				ctx:   context.Background(),
				query: "SELEC",
				args:  nil,
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := memorySQLLite(t, WithDefaultOptions(trace.NewNoopTracerProvider().Tracer("")))
			defer db.Close()

			var dest interface{}

			err := db.GetContext(tt.args.ctx, &dest, tt.args.query, tt.args.args...)
			if !tt.wantErr(t, err, "GetContext()") {
				return
			}

			if err == nil {
				assert.Equal(t, tt.want, dest)
			}
		})
	}
}

func TestDB_BindNamed(t *testing.T) {
	type args struct {
		query string
		arg   interface{}
	}

	tests := []struct {
		name        string
		args        args
		wantBound   string
		wantArglist []interface{}
		wantErr     assert.ErrorAssertionFunc
	}{
		{
			name: "pass",
			args: args{
				query: "SELECT * FROM users WHERE id=:id AND name=:name",
				arg: struct {
					ID   int
					Name string
				}{
					ID:   1,
					Name: "test",
				},
			},
			wantBound:   "SELECT * FROM users WHERE id=? AND name=?",
			wantArglist: []interface{}{1, "test"},
			wantErr:     assert.NoError,
		},
		{
			name: "field not found",
			args: args{
				query: "SELECT * FROM users WHERE id=:id AND name=:name AND status=:status",
				arg: struct {
					ID   int
					Name string
				}{
					ID:   1,
					Name: "test",
				},
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := memorySQLLite(t, WithDefaultOptions(trace.NewNoopTracerProvider().Tracer("")))
			defer db.Close()

			gotBound, gotArglist, err := db.BindNamed(tt.args.query, tt.args.arg)
			if !tt.wantErr(t, err, fmt.Sprintf("BindNamed(%v, %v)", tt.args.query, tt.args.arg)) {
				return
			}

			if err == nil {
				assert.Equalf(t, tt.wantBound, gotBound, "BindNamed(%v, %v)", tt.args.query, tt.args.arg)
				assert.Equalf(t, tt.wantArglist, gotArglist, "BindNamed(%v, %v)", tt.args.query, tt.args.arg)
			}
		})
	}
}

func TestDB_BeginTxx(t *testing.T) {
	type args struct {
		ctx  context.Context
		opts *sql.TxOptions
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "pass without opts",
			args: args{
				ctx:  context.Background(),
				opts: nil,
			},
			wantErr: assert.NoError,
		},
		{
			name: "pass read only",
			args: args{
				ctx:  context.Background(),
				opts: &sql.TxOptions{ReadOnly: true},
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := memorySQLLite(t, WithDefaultOptions(trace.NewNoopTracerProvider().Tracer("")))
			defer db.Close()

			gotTx, err := db.BeginTxx(tt.args.ctx, tt.args.opts)
			if !tt.wantErr(t, err, fmt.Sprintf("BeginTxx(%v, %v)", tt.args.ctx, tt.args.opts)) {
				return
			}

			defer gotTx.Rollback()

			if err == nil {
				assert.NotNil(t, gotTx, "BeginTxx(%v, %v) is nil")
			}
		})
	}
}

func TestDB_ExecContext(t *testing.T) {
	type args struct {
		ctx   context.Context
		query string
		args  []interface{}
	}
	tests := []struct {
		name    string
		prepare func(db Database)
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "pass",
			prepare: func(db Database) {
				_, err := db.ExecContext(context.Background(), "CREATE TABLE foo (id INTEGER)")
				require.NoError(t, err)

				_, err = db.ExecContext(context.Background(), "INSERT INTO foo (id) VALUES (1)")
				require.NoError(t, err)
			},
			args: args{
				ctx:   context.Background(),
				query: "UPDATE foo SET id=? WHERE id=?",
				args:  []interface{}{1, 2},
			},
			wantErr: assert.NoError,
		},
		{
			name:    "error",
			prepare: func(db Database) {},
			args: args{
				ctx:   context.Background(),
				query: "UPDATE foo SET id=? WHERE id=?",
				args:  []interface{}{1, 2},
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := memorySQLLite(t, WithDefaultOptions(trace.NewNoopTracerProvider().Tracer("")))
			defer db.Close()

			tt.prepare(db)

			gotResult, err := db.ExecContext(tt.args.ctx, tt.args.query, tt.args.args...)
			if !tt.wantErr(t, err, fmt.Sprintf("ExecContext(%v, %v, %v)", tt.args.ctx, tt.args.query, tt.args.args)) {
				return
			}

			if err == nil {
				assert.NotNil(t, gotResult)
			}
		})
	}
}

func TestDB_NamedExecContext(t *testing.T) {
	type args struct {
		ctx   context.Context
		query string
		arg   interface{}
	}
	tests := []struct {
		name    string
		prepare func(db Database)
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "pass",
			prepare: func(db Database) {
				_, err := db.ExecContext(context.Background(), "CREATE TABLE foo (id INTEGER)")
				require.NoError(t, err)

				_, err = db.ExecContext(context.Background(), "INSERT INTO foo (id) VALUES (1)")
				require.NoError(t, err)
			},
			args: args{
				ctx:   context.Background(),
				query: "UPDATE foo SET id=:new WHERE id=:old",
				arg:   map[string]interface{}{"new": 2, "old": 1},
			},
			wantErr: assert.NoError,
		},
		{
			name:    "error",
			prepare: func(db Database) {},
			args: args{
				ctx:   context.Background(),
				query: "UPDATE foo SET id=:new WHERE id=:old",
				arg:   map[string]interface{}{"new": 2, "old": 1},
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := memorySQLLite(t, WithDefaultOptions(trace.NewNoopTracerProvider().Tracer("")))
			defer db.Close()

			tt.prepare(db)

			gotResult, err := db.NamedExecContext(tt.args.ctx, tt.args.query, tt.args.arg)
			if !tt.wantErr(t, err, fmt.Sprintf("NamedExecContext(%v, %v, %v)", tt.args.ctx, tt.args.query, tt.args.arg)) {
				return
			}

			if err == nil {
				assert.NotNil(t, gotResult)
			}
		})
	}
}

func TestDB_QueryxContext(t *testing.T) {
	type args struct {
		ctx   context.Context
		query string
		args  []interface{}
	}

	tests := []struct {
		name    string
		args    args
		want    [][]interface{}
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "pass",
			args: args{
				ctx:   context.Background(),
				query: "WITH q(number) as (values (1),(2),(3),(4),(5)) SELECT * from q",
				args:  nil,
			},
			want:    [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}, {int64(5)}},
			wantErr: assert.NoError,
		},
		{
			name: "invalid query",
			args: args{
				ctx:   context.Background(),
				query: "SELEC",
				args:  nil,
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := memorySQLLite(t, WithDefaultOptions(trace.NewNoopTracerProvider().Tracer("")))
			defer db.Close()

			gotRows, err := db.QueryxContext(tt.args.ctx, tt.args.query, tt.args.args...)
			if !tt.wantErr(t, err, fmt.Sprintf("QueryxContext(%v, %v, %v)", tt.args.ctx, tt.args.query, tt.args.args)) {
				return
			}

			if err != nil {
				return
			}

			defer gotRows.Close()

			var result [][]interface{}

			for gotRows.Next() {
				list, err := gotRows.SliceScan()
				require.NoError(t, err)

				result = append(result, list)
			}

			if !assert.NoError(t, gotRows.Err()) {
				return
			}

			assert.Equal(t, tt.want, result)
		})
	}
}

func TestDB_NamedQueryContext(t *testing.T) {
	type args struct {
		ctx   context.Context
		query string
		arg   interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    [][]interface{}
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "pass",
			args: args{
				ctx:   context.Background(),
				query: "WITH q(number) as (values (:val1),(:val2),(:val3)) SELECT * from q",
				arg:   map[string]interface{}{"val1": 1, "val2": 2, "val3": 3},
			},
			want:    [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
			wantErr: assert.NoError,
		},
		{
			name: "invalid query",
			args: args{
				ctx:   context.Background(),
				query: "SELEC",
				arg:   struct{}{},
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := memorySQLLite(t, WithDefaultOptions(trace.NewNoopTracerProvider().Tracer("")))
			defer db.Close()

			gotRows, err := db.NamedQueryContext(tt.args.ctx, tt.args.query, tt.args.arg)
			if !tt.wantErr(t, err, fmt.Sprintf("NamedQueryContext(%v, %v, %v)", tt.args.ctx, tt.args.query, tt.args.arg)) {
				return
			}

			if err != nil {
				return
			}

			defer gotRows.Close()

			var result [][]interface{}

			for gotRows.Next() {
				list, err := gotRows.SliceScan()
				require.NoError(t, err)

				result = append(result, list)
			}

			if !assert.NoError(t, gotRows.Err()) {
				return
			}

			assert.Equal(t, tt.want, result)
		})
	}
}

func TestDB_PreparexContext(t *testing.T) {
	type args struct {
		ctx   context.Context
		query string
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "pass",
			args: args{
				ctx:   context.Background(),
				query: "SELECT 1",
			},
			wantErr: assert.NoError,
		},
		{
			name: "pass",
			args: args{
				ctx:   context.Background(),
				query: "SELEC",
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := memorySQLLite(t, WithDefaultOptions(trace.NewNoopTracerProvider().Tracer("")))
			defer db.Close()

			gotStmt, err := db.PreparexContext(tt.args.ctx, tt.args.query)
			if !tt.wantErr(t, err, fmt.Sprintf("PreparexContext(%v, %v)", tt.args.ctx, tt.args.query)) {
				return
			}

			if err != nil {
				return
			}

			defer gotStmt.Close()

			assert.NotNilf(t, gotStmt, "PreparexContext(%v, %v)", tt.args.ctx, tt.args.query)
		})
	}
}

func TestDB_PrepareNamedContext(t *testing.T) {
	type args struct {
		ctx   context.Context
		query string
	}
	tests := []struct {
		name     string
		args     args
		wantStmt *sqlx.NamedStmt
		wantErr  assert.ErrorAssertionFunc
	}{
		{
			name: "pass",
			args: args{
				ctx:   context.Background(),
				query: "SELECT :number",
			},
			wantErr: assert.NoError,
		},
		{
			name: "pass",
			args: args{
				ctx:   context.Background(),
				query: "SELEC",
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := memorySQLLite(t, WithReconnectHook(), WithPQRetryFunc(0))
			defer db.Close()

			gotStmt, err := db.PrepareNamedContext(tt.args.ctx, tt.args.query)
			if !tt.wantErr(t, err, fmt.Sprintf("PrepareNamedContext(%v, %v)", tt.args.ctx, tt.args.query)) {
				return
			}

			if err != nil {
				return
			}

			defer gotStmt.Close()

			assert.NotNilf(t, gotStmt, "PrepareNamedContext(%v, %v)", tt.args.ctx, tt.args.query)
		})
	}
}

func TestDB_RetryQuery(t *testing.T) {
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

	_, err := db.ExecContext(context.Background(), "SELECT * FROM foo")
	assert.Error(t, err, "db.ExecContext")
	assert.Equal(t, errors, []string{
		"no such table: foo",
		"no such table: foo",
		"no such table: foo",
		"no such table: foo",
		"no such table: foo",
	})
}

func TestDB_RetryQuery2(t *testing.T) {
	var errors []string

	db := memorySQLLite(t, WithRetryPolicy(RetryPolicy{
		MaxAttempts:       5,
		InitialBackoff:    1,
		MaxBackoff:        1,
		BackoffMultiplier: 2,
		ErrIsRetryable: func(err error) bool {
			errors = append(errors, err.Error())

			return true
		},
	}))

	_, err := db.ExecContext(context.Background(), "SELECT * FROM foo")
	assert.Error(t, err, "db.ExecContext")
	assert.Equal(t, errors, []string{
		"no such table: foo",
		"no such table: foo",
		"no such table: foo",
		"no such table: foo",
		"no such table: foo",
	})
}

func TestDB_RetryQueryContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var errs []string

	db := memorySQLLite(t, WithRetryPolicy(RetryPolicy{
		MaxAttempts:       5,
		InitialBackoff:    1,
		MaxBackoff:        1,
		BackoffMultiplier: 1,
		ErrIsRetryable: func(err error) bool {
			if errors.Is(err, context.Canceled) {
				return false
			}

			errs = append(errs, err.Error())
			cancel()

			return true
		},
	}))

	_, err := db.ExecContext(ctx, "SELECT * FROM foo")
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, errs, []string{"no such table: foo"})
}

func memorySQLLite(t *testing.T, opts ...Option) *DB {
	t.Helper()

	db, err := New(&Config{Database: ":memory:", Type: SQLiteDatabase}, opts...)
	require.NoError(t, err)

	return db
}
