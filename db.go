package db

import (
	"database/sql"
	"fmt"
	"strconv"

	"github.com/jmoiron/sqlx"
	"github.com/loghole/dbhook"

	"github.com/loghole/db/internal/dbsqlx"
)

const withHookDriverName = "%s-with-hook"

func New(cfg *Config, options ...Option) (db *sqlx.DB, err error) {
	db = new(sqlx.DB)

	var (
		hooksCfg = cfg.hookConfig()
		builder  = applyOptions(db, hooksCfg, options...)
	)

	hooksCfg.DriverName, err = wrapDriver(cfg.driverName(), builder.hook())
	if err != nil {
		return nil, fmt.Errorf("wrap driver: %w", err)
	}

	tmpDB, err := dbsqlx.NewSQLx(hooksCfg.DriverName, cfg.dataSourceName())
	if err != nil {
		return nil, fmt.Errorf("new db: %w", err)
	}

	*db = *tmpDB

	hooksCfg.Instance = getDBInstans(db)

	return db, nil
}

func getDBInstans(db *sqlx.DB) string {
	var nodeID int

	_ = db.Get(&nodeID, `SHOW node_id`)

	return strconv.Itoa(nodeID)
}

func wrapDriver(driverName string, hook dbhook.Hook) (string, error) {
	// Open db for get original driver.
	db, err := sql.Open(driverName, "")
	if err != nil {
		return "", fmt.Errorf("can't find original driver: %w", err)
	}

	defer db.Close()

	newDriverName := fmt.Sprintf(withHookDriverName, driverName)

	// Register wrapped driver with new name for open it later.
	sql.Register(newDriverName, dbhook.Wrap(db.Driver(), hook))

	return newDriverName, nil
}
