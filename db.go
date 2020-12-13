package db

import (
	"database/sql"
	"fmt"
	"strconv"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/loghole/db/config"
	"github.com/loghole/db/hooks"
	"github.com/loghole/db/internal/dbsqlx"
	"github.com/loghole/dbhook"
	"github.com/opentracing/opentracing-go"
)

const withHookDriverName = "%s-with-hook"

type Config = config.Config

type Option func(b *builder, db *sqlx.DB, cfg *Config)

type builder struct {
	tracerHook    *hooks.TracingHook
	reconnectHook *hooks.ReconnectHook
}

func New(cfg *Config, options ...Option) (*sqlx.DB, error) {
	var (
		err  error
		db   = new(sqlx.DB)
		bldr = applyOptions(db, cfg, options...)
	)

	sql.Register(
		fmt.Sprintf(withHookDriverName, cfg.DriverName()),
		dbhook.Wrap(&pq.Driver{}, bldr.Hooks()),
	)

	db, err = dbsqlx.NewSQLx(cfg.DriverName(), cfg.DataSourceName())
	if err != nil {
		return nil, fmt.Errorf("new db: %w", err)
	}

	bldr.tracerHook.SetDBInstance(getDBInstans(db))

	return db, nil
}

func WithTracingHook(tracer opentracing.Tracer) Option {
	return func(b *builder, db *sqlx.DB, cfg *Config) {
		b.tracerHook = hooks.NewTracingHook(tracer, cfg.Addr, cfg.User, cfg.Database, string(cfg.Type))
	}
}

func WithReconnectHook() Option {
	return func(b *builder, db *sqlx.DB, cfg *Config) {
		b.reconnectHook = hooks.NewReconnectHook(db, cfg)
	}
}

func getDBInstans(db *sqlx.DB) string {
	var nodeID int

	_ = db.Get(&nodeID, `SHOW node_id`)

	return strconv.Itoa(nodeID)
}

func applyOptions(db *sqlx.DB, cfg *Config, options ...Option) *builder {
	b := new(builder)

	for _, option := range options {
		option(b, db, cfg)
	}

	return b
}

func (bldr *builder) Hooks() dbhook.Hook {
	options := make([]dbhook.HookOption, 0)

	if bldr.reconnectHook != nil {
		options = append(options, dbhook.WithHooksError(bldr.reconnectHook))
	}

	if bldr.tracerHook != nil {
		options = append(options,
			dbhook.WithHooksBefore(bldr.tracerHook.Before()),
			dbhook.WithHooksAfter(bldr.tracerHook.After()),
			dbhook.WithHooksError(bldr.tracerHook.After()),
		)
	}

	return dbhook.NewHooks(options...)
}
