package database

import (
	"errors"
	"fmt"

	"github.com/loghole/dbhook"
	"go.opentelemetry.io/otel/trace"

	"github.com/loghole/database/hooks"
	"github.com/loghole/database/internal/helpers"
	"github.com/loghole/database/internal/metrics"
)

const DefaultRetryAttempts = 10

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

func WithTracingHook(tracer trace.Tracer) Option {
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
		b.hookOptions = append(b.hookOptions, dbhook.WithHook(hooks.NewMetricsHook(cfg, collector)))

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

func WithPQRetryFunc(maxAttempts int) Option {
	if maxAttempts == 0 {
		maxAttempts = DefaultRetryAttempts
	}

	return optionFn(func(b *builder, cfg *hooks.Config) error {
		b.retryFunc = func(retryCount int, err error) bool {
			if retryCount >= maxAttempts {
				return false
			}

			return helpers.IsSerialisationFailureErr(err) || errors.Is(err, hooks.ErrCanRetry)
		}

		return nil
	})
}

func WithCockroachRetryFunc() Option {
	return WithPQRetryFunc(DefaultRetryAttempts)
}

func WithDefaultOptions(tracer trace.Tracer) Option {
	return optionFn(func(b *builder, cfg *hooks.Config) error {
		opts := []Option{
			WithTracingHook(tracer),
			WithReconnectHook(),
			WithSimplerrHook(),
			WithPQRetryFunc(DefaultRetryAttempts),
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
