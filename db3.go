package database

import (
	"context"
	"fmt"

	"github.com/loghole/database/internal/dbsqlx"
	"github.com/loghole/database/internal/pool2"
)

type DB3 struct {
	pool pool2.Pool
}

func NewDB3(cfg *Config, options ...Option) (db *DB3, err error) {
	// TODO validate config

	var (
		hooksCfg = cfg.hookConfig()
		builder  = applyOptions(hooksCfg, options...)
	)

	hooksCfg.DriverName, err = wrapDriver(cfg.driverName(), builder.hook())
	if err != nil {
		return nil, fmt.Errorf("wrap driver: %w", err)
	}

	db = &DB3{}

	db.pool, err = pool2.NewClusterPool(2, true, cfg.nodeConfigs2())
	if err != nil {
		return nil, fmt.Errorf("new pool: %w", err)
	}

	return db, nil
}

// SelectContext using this DB.
// Any placeholder parameters are replaced with supplied args.
func (db *DB3) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return db.pool.DoQuery(ctx, func(ctx context.Context, db dbsqlx.Database) error {
		return db.SelectContext(ctx, dest, query, args...)
	})
}

// GetContext using this DB.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (db *DB3) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return db.pool.DoQuery(ctx, func(ctx context.Context, db dbsqlx.Database) error {
		return db.GetContext(ctx, dest, query, args...)
	})
}
