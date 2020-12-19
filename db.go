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

func (d *DB) reconnect() error {
	tmpDB, err := dbsqlx.NewSQLx(d.hooksCfg.DriverName, d.baseCfg.dataSourceName())
	if err != nil {
		return fmt.Errorf("new db: %w", err)
	}

	*d.DB = *tmpDB

	return nil
}
