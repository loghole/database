package db

import (
	"github.com/jmoiron/sqlx"
	"github.com/loghole/dbhook"
	"github.com/opentracing/opentracing-go"

	"github.com/loghole/db/hooks"
)

type Option func(b *builder, db *sqlx.DB, cfg *hooks.Config)

func WithCustomHook(hook dbhook.Hook) Option {
	return func(b *builder, db *sqlx.DB, cfg *hooks.Config) {
		b.hookOptions = append(b.hookOptions, dbhook.WithHook(hook))
	}
}

func WithTracingHook(tracer opentracing.Tracer) Option {
	return func(b *builder, db *sqlx.DB, cfg *hooks.Config) {
		b.hookOptions = append(b.hookOptions, dbhook.WithHook(hooks.NewTracingHook(tracer, cfg)))
	}
}

func WithReconnectHook() Option {
	return func(b *builder, db *sqlx.DB, cfg *hooks.Config) {
		b.hookOptions = append(b.hookOptions, dbhook.WithHooksError(hooks.NewReconnectHook(db, cfg)))
	}
}

type builder struct {
	hookOptions []dbhook.HookOption
}

func applyOptions(db *sqlx.DB, cfg *hooks.Config, options ...Option) *builder {
	b := new(builder)

	for _, option := range options {
		option(b, db, cfg)
	}

	return b
}

func (b *builder) hook() dbhook.Hook {
	return dbhook.NewHooks(b.hookOptions...)
}
