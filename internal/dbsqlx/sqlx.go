package dbsqlx

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
)

type Database interface {
	PingContext(ctx context.Context) error
	BindNamed(query string, arg interface{}) (string, []interface{}, error)
	Beginx() (*sqlx.Tx, error)
	BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error)
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error)
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)
	NamedQueryContext(ctx context.Context, query string, arg interface{}) (*sqlx.Rows, error)
	PreparexContext(ctx context.Context, query string) (*sqlx.Stmt, error)
	PrepareNamedContext(ctx context.Context, query string) (*sqlx.NamedStmt, error)
}

func NewSQLx(driverName, dataSourceName string) (*sqlx.DB, error) {
	stdDB, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("can't open db: %w", err)
	}

	if err := stdDB.PingContext(context.TODO()); err != nil {
		return nil, fmt.Errorf("can't ping db: %w", err)
	}

	db := sqlx.NewDb(stdDB, strings.Split(driverName, "-")[0])

	return db, nil
}
