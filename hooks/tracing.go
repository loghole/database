package hooks

import (
	"context"
	"database/sql"
	"errors"

	"github.com/loghole/dbhook"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	opentracinglog "github.com/opentracing/opentracing-go/log"
)

const (
	dbHost     = "db.host"
	dbDatabase = "db.database"
)

type TracingHook struct {
	tracer opentracing.Tracer
	config *Config
}

func NewTracingHook(tracer opentracing.Tracer, config *Config) *TracingHook {
	return &TracingHook{
		tracer: tracer,
		config: config,
	}
}

func (hook *TracingHook) Before(ctx context.Context, input *dbhook.HookInput) (context.Context, error) {
	if parent := opentracing.SpanFromContext(ctx); parent != nil {
		span := hook.tracer.StartSpan(hook.buildSpanName(input.Caller), opentracing.ChildOf(parent.Context()))

		ext.DBUser.Set(span, hook.config.User)
		ext.DBType.Set(span, hook.config.Type)
		span.SetTag(dbDatabase, hook.config.Database)
		ext.DBInstance.Set(span, hook.config.Instance)
		span.SetTag(dbHost, hook.config.Addr)

		ext.DBStatement.Set(span, input.Query)

		ext.SpanKindRPCClient.Set(span)

		return opentracing.ContextWithSpan(ctx, span), nil
	}

	return ctx, nil
}

func (hook *TracingHook) After(ctx context.Context, input *dbhook.HookInput) (context.Context, error) {
	return hook.finish(ctx, input)
}

func (hook *TracingHook) Error(ctx context.Context, input *dbhook.HookInput) (context.Context, error) {
	return hook.finish(ctx, input)
}

func (hook *TracingHook) finish(ctx context.Context, input *dbhook.HookInput) (context.Context, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		defer span.Finish()

		// If context canceled skip error
		if ctx.Err() != nil && errors.Is(ctx.Err(), context.Canceled) {
			return ctx, input.Error
		}

		// Or err is nil or no rows similarly skip error
		if input.Error == nil || errors.Is(input.Error, sql.ErrNoRows) {
			return ctx, nil
		}

		ext.Error.Set(span, true)
		span.LogFields(opentracinglog.Error(input.Error))
	}

	return ctx, input.Error
}

func (hook *TracingHook) buildSpanName(action dbhook.CallerType) string {
	return "SQL" + " " + string(action)
}
