package database

import (
	"fmt"

	"github.com/loghole/dbhook"
	"github.com/opentracing/opentracing-go"

	"github.com/loghole/database/hooks"
	"github.com/loghole/database/internal/helpers"
	"github.com/loghole/database/internal/metrics"
)

type Option interface {
	apply(b *builder, cfg *hooks.Config) error
}

type optionFn func(b *builder, cfg *hooks.Config) error

func (fn optionFn) apply(b *builder, cfg *hooks.Config) error {
	return fn(b, cfg)
}

func WithCustomHook(hook dbhook.Hook) Option {
	return optionFn(func(b *builder, cfg *hooks.Config) error {
		b.hookOptions = append(b.hookOptions, dbhook.WithHook(hook))

		return nil
	})
}

func WithTracingHook(tracer opentracing.Tracer) Option {
	return optionFn(func(b *builder, cfg *hooks.Config) error {
		b.hookOptions = append(b.hookOptions, dbhook.WithHook(hooks.NewTracingHook(tracer, cfg)))

		return nil
	})
}

func WithReconnectHook() Option {
	return optionFn(func(b *builder, cfg *hooks.Config) error {
		b.hookOptions = append(b.hookOptions, dbhook.WithHooksError(hooks.NewReconnectHook(cfg)))

		return nil
	})
}

func WithSimplerrHook() Option {
	return optionFn(func(b *builder, cfg *hooks.Config) error {
		b.hookOptions = append(b.hookOptions, dbhook.WithHooksError(hooks.NewSimplerrHook()))

		return nil
	})
}

func WithMetricsHook(collector hooks.MetricCollector) Option {
	return optionFn(func(b *builder, cfg *hooks.Config) error {
		hook := hooks.NewMetricsHook(cfg, collector)

		b.hookOptions = append(
			b.hookOptions,
			dbhook.WithHooksBefore(hook),
			dbhook.WithHooksAfter(hook),
		)

		return nil
	})
}

func WithPrometheusMetrics() Option {
	return optionFn(func(b *builder, cfg *hooks.Config) error {
		collector, err := metrics.NewMetrics()
		if err != nil {
			return fmt.Errorf("init prometheus collector: %w", err)
		}

		hook := hooks.NewMetricsHook(cfg, collector)

		b.hookOptions = append(
			b.hookOptions,
			dbhook.WithHooksBefore(hook),
			dbhook.WithHooksAfter(hook),
		)

		return nil
	})
}

func WithRetryFunc(f RetryFunc) Option {
	return optionFn(func(b *builder, cfg *hooks.Config) error {
		b.retryFunc = f

		return nil
	})
}

func WithCockroachRetryFunc() Option {
	return optionFn(func(b *builder, cfg *hooks.Config) error {
		b.retryFunc = func(_ int, err error) bool {
			return helpers.IsSerialisationFailureErr(err)
		}

		return nil
	})
}

func WithDefaultOptions(tracer opentracing.Tracer) Option {
	return optionFn(func(b *builder, cfg *hooks.Config) error {
		opts := []Option{
			WithTracingHook(tracer),
			WithReconnectHook(),
			WithSimplerrHook(),
			WithCockroachRetryFunc(),
		}

		for _, opt := range opts {
			if err := opt.apply(b, cfg); err != nil {
				return err // nolint:wrapcheck // need clean err.
			}
		}

		return nil
	})
}

type builder struct {
	retryFunc   RetryFunc
	hookOptions []dbhook.HookOption
}

func applyOptions(cfg *hooks.Config, options ...Option) (*builder, error) {
	b := new(builder)

	for _, opt := range options {
		if err := opt.apply(b, cfg); err != nil {
			return nil, err // nolint:wrapcheck // need clean err.
		}
	}

	return b, nil
}

func (b *builder) hook() dbhook.Hook {
	if len(b.hookOptions) == 0 {
		return nil
	}

	return dbhook.NewHooks(b.hookOptions...)
}
