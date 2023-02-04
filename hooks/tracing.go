package hooks

import (
	"context"
	"database/sql"
	"errors"

	"github.com/loghole/dbhook"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	"go.opentelemetry.io/otel/trace"
)

type TracingHook struct {
	tracer trace.Tracer
	config *Config
}

func NewTracingHook(tracer trace.Tracer, config *Config) *TracingHook {
	return &TracingHook{
		tracer: tracer,
		config: config,
	}
}

func (hook *TracingHook) Before(ctx context.Context, input *dbhook.HookInput) (context.Context, error) {
	ctx, span := hook.tracer.Start(ctx, hook.buildSpanName(input.Caller), trace.WithSpanKind(trace.SpanKindInternal))

	span.SetAttributes(
		semconv.DBUserKey.String(hook.config.User),
		semconv.DBSystemKey.String(hook.config.Type),
		semconv.DBNameKey.String(hook.config.Database),
		semconv.DBStatementKey.String(input.Query),
		semconv.HostIDKey.String(hook.config.Instance),
		semconv.HostNameKey.String(hook.config.Addr),
	)

	return ctx, nil
}

func (hook *TracingHook) After(ctx context.Context, input *dbhook.HookInput) (context.Context, error) {
	return hook.finish(ctx, input)
}

func (hook *TracingHook) Error(ctx context.Context, input *dbhook.HookInput) (context.Context, error) {
	return hook.finish(ctx, input)
}

func (hook *TracingHook) finish(ctx context.Context, input *dbhook.HookInput) (context.Context, error) {
	if span := trace.SpanFromContext(ctx); span != nil {
		defer span.End()

		// If context canceled skip error
		if ctx.Err() != nil && errors.Is(ctx.Err(), context.Canceled) {
			return ctx, input.Error
		}

		// Or err is nil or no rows similarly skip error
		if input.Error == nil || errors.Is(input.Error, sql.ErrNoRows) {
			return ctx, input.Error
		}

		span.RecordError(input.Error)
		span.SetStatus(codes.Error, "error")
	}

	return ctx, input.Error
}

func (hook *TracingHook) buildSpanName(action dbhook.CallerType) string {
	return "SQL " + string(action)
}
