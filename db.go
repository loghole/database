package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/loghole/dbhook"

	"github.com/loghole/database/hooks"
	"github.com/loghole/database/internal/dbsqlx"
)

const (
	_withHookDriverName = "%s-with-hook-%s"
	_txSpanName         = "SQL Tx"
	_defaultTracerName  = "github.com/loghole/database"
)

var (
	ErrMaxRetryAttempts = errors.New("max retry attempts has been reached")
	ErrInvalidConfig    = errors.New("invalid config")
)

type DB struct {
	DB       *sqlx.DB
	hooksCfg *hooks.Config
	baseCfg  *Config

	options options
}

type (
	TransactionFunc func(ctx context.Context, tx *sqlx.Tx) error
	QueryFunc       func(ctx context.Context, db *sqlx.DB) error
	RetryFunc       func(retryCount int, err error) bool
)

func New(cfg *Config, opts ...Option) (db *DB, err error) {
	db = &DB{
		baseCfg:  cfg,
		hooksCfg: cfg.hookConfig(),
	}

	if err := db.options.apply(db.hooksCfg, opts...); err != nil {
		return nil, fmt.Errorf("apply options: %w", err)
	}

	db.hooksCfg.DriverName, err = wrapDriver(cfg.driverName(), db.options.hook())
	if err != nil {
		return nil, fmt.Errorf("wrap driver: %w", err)
	}

	db.DB, err = dbsqlx.NewSQLx(db.hooksCfg.DriverName, cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("new db: %w", err)
	}

	db.hooksCfg.Instance = getDBIInstance(db.DB)
	db.hooksCfg.ReconnectFn = db.reconnect

	return db, nil
}

// Close closes the database and prevents new queries from starting.
// Close then waits for all queries that have started processing on the server
// to finish.
//
// It is rare to Close a DB, as the DB handle is meant to be
// long-lived and shared between many goroutines.
func (db *DB) Close() error {
	return db.DB.Close()
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
	tmpSQLx, err := dbsqlx.NewSQLx(db.hooksCfg.DriverName, db.baseCfg.DSN())
	if err != nil {
		return fmt.Errorf("new db: %w", err)
	}

	oldDB := *db.DB
	*db.DB = *tmpSQLx

	go oldDB.Close()

	return nil
}

func getDBIInstance(db *sqlx.DB) string {
	var nodeID int

	_ = db.Get(&nodeID, `SHOW node_id`)

	return strconv.Itoa(nodeID)
}
