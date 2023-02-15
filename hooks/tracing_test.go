package hooks

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/loghole/dbhook"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.13.0"
)

func TestTracingHook(t *testing.T) {
	ctx := context.Background()

	type args struct {
		config *Config
	}
	tests := []struct {
		name       string
		args       args
		do         func(hook *TracingHook)
		wantAttrs  []attribute.KeyValue
		wantStatus codes.Code
	}{
		{
			name: "base query",
			args: args{
				config: &Config{
					Addr:     "127.0.0.1:5432",
					User:     "test",
					Database: "postgresdb",
					Type:     "postgres",
					Instance: "1",
				},
			},
			do: func(hook *TracingHook) {
				input := &dbhook.HookInput{
					Query:  "SELECT id FROM users",
					Caller: dbhook.CallerQuery,
				}

				ctx, _ = hook.Before(ctx, input)
				ctx, _ = hook.After(ctx, input)
			},
			wantAttrs: []attribute.KeyValue{
				semconv.DBUserKey.String("test"),
				semconv.DBSystemKey.String("postgres"),
				semconv.DBNameKey.String("postgresdb"),
				semconv.DBStatementKey.String("SELECT id FROM users"),
				semconv.HostIDKey.String("1"),
				semconv.HostNameKey.String("127.0.0.1:5432"),
			},
			wantStatus: codes.Unset,
		},
		{
			name: "failed query",
			args: args{
				config: &Config{
					Addr:     "127.0.0.1:5432",
					User:     "test",
					Database: "postgresdb",
					Type:     "postgres",
					Instance: "1",
				},
			},
			do: func(hook *TracingHook) {
				input := &dbhook.HookInput{
					Query:  "INSERT INTO users (id) VALUES ($1)",
					Caller: dbhook.CallerQuery,
				}

				ctx, _ = hook.Before(ctx, input)

				input.Error = errors.New("some error")

				ctx, _ = hook.Error(ctx, input)
			},
			wantAttrs: []attribute.KeyValue{
				semconv.DBUserKey.String("test"),
				semconv.DBSystemKey.String("postgres"),
				semconv.DBNameKey.String("postgresdb"),
				semconv.DBStatementKey.String("INSERT INTO users (id) VALUES ($1)"),
				semconv.HostIDKey.String("1"),
				semconv.HostNameKey.String("127.0.0.1:5432"),
			},
			wantStatus: codes.Error,
		},
		{
			name: "sql no rows",
			args: args{
				config: &Config{
					Addr:     "127.0.0.1:5432",
					User:     "test",
					Database: "postgresdb",
					Type:     "postgres",
					Instance: "1",
				},
			},
			do: func(hook *TracingHook) {
				input := &dbhook.HookInput{
					Query:  "INSERT INTO users (id) VALUES ($1)",
					Caller: dbhook.CallerQuery,
				}

				ctx, _ = hook.Before(ctx, input)

				input.Error = sql.ErrNoRows

				ctx, _ = hook.Error(ctx, input)
			},
			wantAttrs: []attribute.KeyValue{
				semconv.DBUserKey.String("test"),
				semconv.DBSystemKey.String("postgres"),
				semconv.DBNameKey.String("postgresdb"),
				semconv.DBStatementKey.String("INSERT INTO users (id) VALUES ($1)"),
				semconv.HostIDKey.String("1"),
				semconv.HostNameKey.String("127.0.0.1:5432"),
			},
			wantStatus: codes.Unset,
		},
		{
			name: "context canceled",
			args: args{
				config: &Config{
					Addr:     "127.0.0.1:5432",
					User:     "test",
					Database: "postgresdb",
					Type:     "postgres",
					Instance: "1",
				},
			},
			do: func(hook *TracingHook) {
				input := &dbhook.HookInput{
					Query:  "INSERT INTO users (id) VALUES ($1)",
					Caller: dbhook.CallerQuery,
				}

				ctx, _ = hook.Before(ctx, input)

				ctx, cancel := context.WithCancel(ctx)
				cancel()

				ctx, _ = hook.Error(ctx, input)
			},
			wantAttrs: []attribute.KeyValue{
				semconv.DBUserKey.String("test"),
				semconv.DBSystemKey.String("postgres"),
				semconv.DBNameKey.String("postgresdb"),
				semconv.DBStatementKey.String("INSERT INTO users (id) VALUES ($1)"),
				semconv.HostIDKey.String("1"),
				semconv.HostNameKey.String("127.0.0.1:5432"),
			},
			wantStatus: codes.Unset,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				recorder = tracetest.NewSpanRecorder()
				tracer   = tracesdk.NewTracerProvider(tracesdk.WithSpanProcessor(recorder)).Tracer("")
			)

			hook := NewTracingHook(tracer, tt.args.config)
			tt.do(hook)

			if !assert.Len(t, recorder.Ended(), 1, "invalid spans count") {
				return
			}

			span := recorder.Ended()[0]

			assert.Equalf(t, tt.wantAttrs, span.Attributes(), "invalid span attrs: got=%v, want=%v", tt.wantAttrs, span.Attributes())
			assert.Equalf(t, tt.wantStatus, span.Status().Code, "invalid span status: got=%v, want=%v", tt.wantStatus, span.Status().Code)
		})
	}
}
