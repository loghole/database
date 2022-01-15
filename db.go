package database

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/loghole/dbhook"

	"github.com/loghole/database/hooks"
	"github.com/loghole/database/internal/dbsqlx"
)

const (
	_withHookDriverName  = "%s-with-hook-%s"
	_transactionSpanName = "SQL Tx"
	_tracerName          = "github.com/loghole/database"
)

type DB struct {
	DB        *sqlx.DB
	retryFunc RetryFunc
	hooksCfg  *hooks.Config
	baseCfg   *Config
}

type (
	TransactionFunc func(ctx context.Context, tx *sqlx.Tx) error
	QueryFunc       func(ctx context.Context, db *sqlx.DB) error
	RetryFunc       func(retryCount int, err error) bool
)

func New(cfg *Config, options ...Option) (db *DB, err error) {
	hooksCfg := cfg.hookConfig()

	builder, err := applyOptions(hooksCfg, options...)
	if err != nil {
		return nil, fmt.Errorf("apply options: %w", err)
	}

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

func (db *DB) Close() error {
	return db.DB.Close()
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

	// nolint:gomnd // num base ok
	newDriverName := fmt.Sprintf(_withHookDriverName, driverName, strconv.FormatInt(time.Now().UnixNano(), 36))

	// Register wrapped driver with new name for open it later.
	sql.Register(newDriverName, dbhook.Wrap(db.Driver(), hook))

	return newDriverName, nil
}

func (db *DB) reconnect() error {
	tmpSQLx, err := dbsqlx.NewSQLx(db.hooksCfg.DriverName, db.baseCfg.dataSourceName())
	if err != nil {
		return fmt.Errorf("new db: %w", err)
	}

	*db.DB = *tmpSQLx

	return nil
}
