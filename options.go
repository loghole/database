package database

import (
	"errors"
	"fmt"
	"time"

	"github.com/loghole/dbhook"
	"go.opentelemetry.io/otel/trace"

	"github.com/loghole/database/hooks"
	"github.com/loghole/database/internal/helpers"
	"github.com/loghole/database/internal/metrics"
)

const (
	DefaultRetryAttempts          = 10
	DefaultRetryInitialBackoff    = time.Millisecond
	DefaultRetryMaxBackoff        = time.Millisecond * 100
	DefaultRetryBackoffMultiplier = 1.5
)

type options struct {
	retryPolicy *RetryPolicy
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

func WithPQRetryFunc(maxAttempts int) Option {
	if maxAttempts == 0 {
		maxAttempts = DefaultRetryAttempts
	}

	return WithRetryPolicy(RetryPolicy{
		MaxAttempts:       maxAttempts,
		InitialBackoff:    DefaultRetryInitialBackoff,
		MaxBackoff:        DefaultRetryMaxBackoff,
		BackoffMultiplier: DefaultRetryBackoffMultiplier,
		ErrIsRetryable: func(err error) bool {
			return helpers.IsSerialisationFailureErr(err) || errors.Is(err, hooks.ErrCanRetry)
		},
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

// RetryPolicy defines retry policy for queries.
//
// nolint:govet // not need for config.
type RetryPolicy struct {
	// MaxAttempts is the maximum number of attempts.
	//
	// This field is required and must be two or greater.
	MaxAttempts int

	// Exponential backoff parameters. The initial retry attempt will occur at
	// random(0, InitialBackoff). In general, the nth attempt will occur at
	// random(0, min(InitialBackoff*BackoffMultiplier**(n-1), MaxBackoff)).
	//
	// These fields are required and must be greater than zero.
	InitialBackoff    time.Duration
	MaxBackoff        time.Duration
	BackoffMultiplier float64

	// Reports when error is retryable.
	//
	// This field is required and must be non-empty.
	ErrIsRetryable func(err error) bool
}

func (rp *RetryPolicy) validate() error {
	if rp.MaxAttempts <= 1 {
		return fmt.Errorf("%w: RetryPolicy: MaxAttempts must be two or greater", ErrInvalidConfig)
	}

	if rp.InitialBackoff < 0 {
		return fmt.Errorf("%w: RetryPolicy: InitialBackoff must be greater than zero", ErrInvalidConfig)
	}

	if rp.MaxBackoff < 0 {
		return fmt.Errorf("%w: RetryPolicy: MaxBackoff must be greater than zero", ErrInvalidConfig)
	}

	if rp.BackoffMultiplier < 0 {
		return fmt.Errorf("%w: RetryPolicy: BackoffMultiplier must be greater than zero", ErrInvalidConfig)
	}

	if rp.ErrIsRetryable == nil {
		return fmt.Errorf("%w: RetryPolicy: ErrIsRetryable required and must be non-empty", ErrInvalidConfig)
	}

	return nil
}

func WithRetryPolicy(retryPolicy RetryPolicy) Option {
	return newFuncOption(func(opts *options, cfg *hooks.Config) error {
		if err := retryPolicy.validate(); err != nil {
			return err
		}

		opts.retryPolicy = &retryPolicy

		return nil
	})
}
