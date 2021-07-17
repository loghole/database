package database

import (
	"errors"

	"github.com/lib/pq"
	"github.com/loghole/dbhook"
	"github.com/opentracing/opentracing-go"

	"github.com/loghole/database/hooks"
)

type Option func(b *builder, cfg *hooks.Config)

func WithCustomHook(hook dbhook.Hook) Option {
	return func(b *builder, cfg *hooks.Config) {
		b.hookOptions = append(b.hookOptions, dbhook.WithHook(hook))
	}
}

func WithTracingHook(tracer opentracing.Tracer) Option {
	return func(b *builder, cfg *hooks.Config) {
		b.hookOptions = append(b.hookOptions, dbhook.WithHook(hooks.NewTracingHook(tracer, cfg)))
	}
}

func WithReconnectHook() Option {
	return func(b *builder, cfg *hooks.Config) {
		b.hookOptions = append(b.hookOptions, dbhook.WithHooksError(hooks.NewReconnectHook(cfg)))
	}
}

func WithSimplerrHook() Option {
	return func(b *builder, cfg *hooks.Config) {
		b.hookOptions = append(b.hookOptions, dbhook.WithHooksError(hooks.NewSimplerrHook()))
	}
}

func WithRetryFunc(f RetryFunc) Option {
	return func(b *builder, cfg *hooks.Config) {
		b.retryFunc = f
	}
}

func WithCockroachRetryFunc() Option {
	// Cockroach retryable transaction code
	const retryableCode = "40001"

	return func(b *builder, cfg *hooks.Config) {
		b.retryFunc = func(err error) bool {
			var pqErr pq.Error

			if errors.As(err, &pqErr) {
				return pqErr.Code == retryableCode
			}

			return false
		}
	}
}

func WithDefaultOptions(tracer opentracing.Tracer) Option {
	return func(b *builder, cfg *hooks.Config) {
		opts := []Option{
			WithTracingHook(tracer),
			WithReconnectHook(),
			WithSimplerrHook(),
			WithCockroachRetryFunc(),
		}

		for _, fn := range opts {
			fn(b, cfg)
		}
	}
}

type builder struct {
	retryFunc   RetryFunc
	hookOptions []dbhook.HookOption
}

func applyOptions(cfg *hooks.Config, options ...Option) *builder {
	b := new(builder)

	for _, option := range options {
		option(b, cfg)
	}

	return b
}

func (b *builder) hook() dbhook.Hook {
	if len(b.hookOptions) == 0 {
		return nil
	}

	return dbhook.NewHooks(b.hookOptions...)
}
