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

type options struct {
	retryFunc   RetryFunc
	hookOptions []dbhook.HookOption
}

func (o *options) apply(cfg *hooks.Config, opts ...Option) error {
	for _, opt := range opts {
		if err := opt.apply(o, cfg); err != nil {
			return err // nolint:wrapcheck // need clean err.
		}
	}

	return nil
}

func (o *options) hook() dbhook.Hook {
	if len(o.hookOptions) == 0 {
		return nil
	}

	return dbhook.NewHooks(o.hookOptions...)
}

// Option sets options such as hooks, metrics and retry parameters, etc.
type Option interface {
	apply(b *options, cfg *hooks.Config) error
}

type funcOption struct {
	fn func(b *options, cfg *hooks.Config) error
}

func (o *funcOption) apply(opts *options, cfg *hooks.Config) error {
	return o.fn(opts, cfg)
}

func newFuncOption(fn func(opts *options, cfg *hooks.Config) error) Option {
	return &funcOption{fn: fn}
}

func WithCustomHook(hook dbhook.Hook) Option {
	return newFuncOption(func(opts *options, cfg *hooks.Config) error {
		opts.hookOptions = append(opts.hookOptions, dbhook.WithHook(hook))

		return nil
	})
}

func WithTracingHook(tracer trace.Tracer) Option {
	return newFuncOption(func(opts *options, cfg *hooks.Config) error {
		opts.hookOptions = append(opts.hookOptions, dbhook.WithHook(hooks.NewTracingHook(tracer, cfg)))

		return nil
	})
}

func WithReconnectHook() Option {
	return newFuncOption(func(opts *options, cfg *hooks.Config) error {
		opts.hookOptions = append(opts.hookOptions, dbhook.WithHooksError(hooks.NewReconnectHook(cfg)))

		return nil
	})
}

func WithSimplerrHook() Option {
	return newFuncOption(func(opts *options, cfg *hooks.Config) error {
		opts.hookOptions = append(opts.hookOptions, dbhook.WithHooksError(hooks.NewSimplerrHook()))

		return nil
	})
}

func WithMetricsHook(collector hooks.MetricCollector) Option {
	return newFuncOption(func(opts *options, cfg *hooks.Config) error {
		opts.hookOptions = append(opts.hookOptions, dbhook.WithHook(hooks.NewMetricsHook(cfg, collector)))

		return nil
	})
}

func WithPrometheusMetrics() Option {
	return newFuncOption(func(opts *options, cfg *hooks.Config) error {
		collector, err := metrics.NewMetrics()
		if err != nil {
			return fmt.Errorf("init prometheus collector: %w", err)
		}

		hook := hooks.NewMetricsHook(cfg, collector)

		opts.hookOptions = append(
			opts.hookOptions,
			dbhook.WithHooksBefore(hook),
			dbhook.WithHooksAfter(hook),
		)

		return nil
	})
}

func WithRetryFunc(f RetryFunc) Option {
	return newFuncOption(func(opts *options, cfg *hooks.Config) error {
		opts.retryFunc = f

		return nil
	})
}

func WithPQRetryFunc(maxAttempts int) Option {
	if maxAttempts == 0 {
		maxAttempts = DefaultRetryAttempts
	}

	return newFuncOption(func(opts *options, cfg *hooks.Config) error {
		opts.retryFunc = func(retryCount int, err error) bool {
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
	return newFuncOption(func(opts *options, cfg *hooks.Config) error {
		list := []Option{
			WithTracingHook(tracer),
			WithReconnectHook(),
			WithSimplerrHook(),
			WithPQRetryFunc(DefaultRetryAttempts),
			WithPrometheusMetrics(),
		}

		for _, opt := range list {
			if err := opt.apply(opts, cfg); err != nil {
				return err // nolint:wrapcheck // need clean err.
			}
		}

		return nil
	})
}
