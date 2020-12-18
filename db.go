package database

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/loghole/dbhook"
	"github.com/opentracing/opentracing-go"

	"github.com/loghole/database/hooks"
	"github.com/loghole/database/internal/dbsqlx"
)

const (
	withHookDriverName  = "%s-with-hook-%s"
	transactionSpanName = "SQL Tx"
)

type DB struct {
	*sqlx.DB
	retryFunc RetryFunc
	hooksCfg  *hooks.Config
	baseCfg   *Config
}

type (
	TransactionFunc func(ctx context.Context, tx *sqlx.Tx) error
	RetryFunc       func(err error) bool
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

	// Retry transaction for cockroach db.
	for {
		if err = fn(ctx, tx); !d.errIsRetryable(err) {
			break
		}
	}

	if err != nil {
		return err //nolint:wrapcheck // need clear error
	}

	if err := tx.Commit(); err != nil {
		return err //nolint:wrapcheck // need clear error
	}

	return nil
}

func (d *DB) rollback(tx *sqlx.Tx) {
	_ = tx.Rollback()
}

func (d *DB) errIsRetryable(err error) bool {
	if d.retryFunc != nil {
		return d.retryFunc(err)
	}

	return false
}

func (d *DB) reconnect() error {
	tmpDB, err := dbsqlx.NewSQLx(d.hooksCfg.DriverName, d.baseCfg.dataSourceName())
	if err != nil {
		return fmt.Errorf("new db: %w", err)
	}

	*d.DB = *tmpDB

	return nil
}

func New(cfg *Config, options ...Option) (db *DB, err error) {
	var (
		hooksCfg = cfg.hookConfig()
		builder  = applyOptions(hooksCfg, options...)
	)

	hooksCfg.DriverName, err = wrapDriver(cfg.driverName(), builder.hook())
	if err != nil {
		return nil, fmt.Errorf("wrap driver: %w", err)
	}

	db = &DB{
		retryFunc: builder.retryFunc,
		hooksCfg:  hooksCfg,
		baseCfg:   cfg,
	}

	db.DB, err = dbsqlx.NewSQLx(hooksCfg.DriverName, cfg.dataSourceName())
	if err != nil {
		return nil, fmt.Errorf("new db: %w", err)
	}

	hooksCfg.Instance = getDBInstans(db.DB)
	hooksCfg.ReconnectFn = db.reconnect

	return db, nil
}

func getDBInstans(db *sqlx.DB) string {
	var nodeID int

	_ = db.Get(&nodeID, `SHOW node_id`)

	return strconv.Itoa(nodeID)
}

func wrapDriver(driverName string, hook dbhook.Hook) (string, error) {
	if hook == nil { // skip wrapping for empty hook.
		return driverName, nil
	}

	// Open db for get original driver.
	db, err := sql.Open(driverName, "")
	if err != nil {
		return "", fmt.Errorf("can't find original driver: %w", err)
	}

	defer db.Close()

	newDriverName := fmt.Sprintf(withHookDriverName, driverName, strconv.FormatInt(time.Now().UnixNano(), 36))

	// Register wrapped driver with new name for open it later.
	sql.Register(newDriverName, dbhook.Wrap(db.Driver(), hook))

	return newDriverName, nil
}
