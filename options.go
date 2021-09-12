package database

import (
	"context"
	"errors"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/loghole/dbhook"
	"github.com/opentracing/opentracing-go"

	"github.com/loghole/database/hooks"
)

type (
	TransactionFunc func(ctx context.Context, tx *sqlx.Tx) error
	RetryFunc       func(retryCount int, err error) bool
)

type Option func(b *builder)

func WithCustomHook(hook dbhook.Hook) Option {
	return func(b *builder) {
		b.hookOptions = append(b.hookOptions, dbhook.WithHook(hook))
	}
}

func WithTracing(tracer opentracing.Tracer) Option {
	return func(b *builder) {
		b.tracer = tracer
	}
}

/*
func WithReconnectHook() Option {
	return func(b *builder) {
		b.hookOptions = append(b.hookOptions, dbhook.WithHooksError(hooks.NewReconnectHook(cfg)))
	}
}*/

func WithSimplerrHook() Option {
	return func(b *builder) {
		b.hookOptions = append(b.hookOptions, dbhook.WithHooksError(hooks.NewSimplerrHook()))
	}
}

func WithRetryFunc(f RetryFunc) Option {
	return func(b *builder) {
		b.retryFunc = f
	}
}

func WithCockroachRetryFunc() Option {
	// Cockroach retryable transaction code
	const retryableCode = "40001"

	return func(b *builder) {
		b.retryFunc = func(_ int, err error) bool {
			var pqErr pq.Error

			if errors.As(err, &pqErr) {
				return pqErr.Code == retryableCode
			}

			return false
		}
	}
}

func WithDefaultOptions(tracer opentracing.Tracer) Option {
	return func(b *builder) {
		opts := []Option{
			WithTracing(tracer),
			WithSimplerrHook(),
			WithCockroachRetryFunc(),
		}

		for _, fn := range opts {
			fn(b)
		}
	}
}

type builder struct {
	retryFunc   RetryFunc
	hookOptions []dbhook.HookOption
	tracer      opentracing.Tracer
}

func applyOptions(options ...Option) *builder {
	b := new(builder)

	for _, option := range options {
		option(b)
	}

	return b
}

func (b *builder) hook() dbhook.Hook {
	if len(b.hookOptions) == 0 {
		return nil
	}

	return dbhook.NewHooks(b.hookOptions...)
}
