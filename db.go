package database

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/loghole/database/internal/pool"
	"github.com/loghole/dbhook"
)

const (
	withHookDriverName  = "%s-with-hook-%s"
	transactionSpanName = "SQL Tx"
)

type DB struct {
	pool pool.Pool

	retryFunc RetryFunc
}

func New(cfg *Config, options ...Option) (db *DB, err error) {
	nodeConfigs, err := cfg.buildNodeConfigs()
	if err != nil {
		return nil, fmt.Errorf("build node configs: %w", err)
	}

	builder := applyOptions(options...)

	driverName, err := wrapDriver(cfg.Type.DriverName(), builder.hook())
	if err != nil {
		return nil, fmt.Errorf("wrap driver: %w", err)
	}

	db = &DB{
		retryFunc: builder.retryFunc,
	}

	db.pool, err = pool.NewClusterPool(
		driverName,
		cfg.ActiveCount,
		cfg.CanUseOtherLevel,
		nodeConfigs,
	)
	if err != nil {
		return nil, fmt.Errorf("new pool: %w", err)
	}

	return db, nil
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
	newDriverName := fmt.Sprintf(withHookDriverName, driverName, strconv.FormatInt(time.Now().UnixNano(), 36))

	// Register wrapped driver with new name for open it later.
	sql.Register(newDriverName, dbhook.Wrap(db.Driver(), hook))

	return newDriverName, nil
}
