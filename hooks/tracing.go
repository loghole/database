package hooks

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/loghole/dbhook"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	opentracinglog "github.com/opentracing/opentracing-go/log"
)

const (
	dbHost     = "db.host"
	dbDatabase = "db.database"
)

type TracingHookConfig struct {
	host       string
	user       string
	db         string
	dbType     string
	dbInstance string
}

type TracingHook struct {
	tracer opentracing.Tracer
	config TracingHookConfig
}

func NewTracingHook(tracer opentracing.Tracer, host, user, db, dbType string) *TracingHook {
	return &TracingHook{
		tracer: tracer,
		config: TracingHookConfig{
			host:   host,
			user:   user,
			db:     db,
			dbType: dbType,
		},
	}
}

func (hook *TracingHook) SetDBInstance(instance string) {
	if hook == nil {
		return
	}

	hook.config.dbInstance = instance
}

func (hook *TracingHook) HookOptions() []dbhook.HookOption {
	return []dbhook.HookOption{
		dbhook.WithHooksBefore(hook.Before()),
		dbhook.WithHooksAfter(hook.After()),
		dbhook.WithHooksError(hook.After()),
	}
}

func (hook *TracingHook) Before() dbhook.HookCallFunc {
	return func(ctx context.Context, input *dbhook.HookInput) (context.Context, error) {
		if parent := opentracing.SpanFromContext(ctx); parent != nil {
			span := hook.tracer.StartSpan(hook.buildSpanName(input.Caller), opentracing.ChildOf(parent.Context()))

			ext.DBUser.Set(span, hook.config.user)
			ext.DBType.Set(span, hook.config.dbType)
			span.SetTag(dbDatabase, hook.config.db)
			ext.DBInstance.Set(span, hook.config.dbInstance)
			span.SetTag(dbHost, hook.config.host)

			ext.DBStatement.Set(span, input.Query)

			ext.SpanKindRPCClient.Set(span)

			return opentracing.ContextWithSpan(ctx, span), nil
		}

		return ctx, nil
	}
}

func (hook *TracingHook) After() dbhook.HookCallFunc {
	return func(ctx context.Context, input *dbhook.HookInput) (context.Context, error) {
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
}

func (hook *TracingHook) buildSpanName(action dbhook.CallerType) string {
	return strings.Join([]string{"SQL", string(action)}, " ")
}
