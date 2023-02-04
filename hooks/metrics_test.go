package hooks

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/lib/pq"
	"github.com/loghole/dbhook"

	"github.com/loghole/database/mocks"
)

func TestMetricsHook(t *testing.T) {
	var (
		ctx  = context.Background()
		ctrl = gomock.NewController(t)
	)

	type args struct {
		config        *Config
		makeCollector func() MetricCollector
	}
	tests := []struct {
		name string
		args args
		do   func(*MetricsHook)
	}{
		{
			name: "base query",
			args: args{
				config: &Config{
					Addr:     "127.0.0.1:5432",
					User:     "test",
					Database: "postgresdb",
					Type:     "postgres",
				},
				makeCollector: func() MetricCollector {
					collector := mocks.NewMockMetricCollector(ctrl)
					collector.EXPECT().QueryDurationObserve(
						"postgres",
						"127.0.0.1:5432",
						"postgresdb",
						"select",
						"users",
						false,
						gomock.Any(),
					)

					return collector
				},
			},
			do: func(hook *MetricsHook) {
				input := &dbhook.HookInput{
					Query:  "SELECT id FROM users",
					Caller: dbhook.CallerQuery,
				}

				ctx, _ = hook.Before(ctx, input)
				ctx, _ = hook.After(ctx, input)
			},
		},
		{
			name: "failed query",
			args: args{
				config: &Config{
					Addr:     "127.0.0.1:5432",
					User:     "test",
					Database: "postgresdb",
					Type:     "postgres",
				},
				makeCollector: func() MetricCollector {
					collector := mocks.NewMockMetricCollector(ctrl)
					collector.EXPECT().QueryDurationObserve(
						"postgres",
						"127.0.0.1:5432",
						"postgresdb",
						"insert",
						"users",
						true,
						gomock.Any(),
					)

					return collector
				},
			},
			do: func(hook *MetricsHook) {
				input := &dbhook.HookInput{
					Query:  "INSERT INTO users (id) VALUES ($1)",
					Caller: dbhook.CallerQuery,
				}

				ctx, _ = hook.Before(ctx, input)

				input.Error = errors.New("some error")

				ctx, _ = hook.Error(ctx, input)
			},
		},
		{
			name: "failed commit",
			args: args{
				config: &Config{
					Addr:     "127.0.0.1:5432",
					User:     "test",
					Database: "postgresdb",
					Type:     "postgres",
				},
				makeCollector: func() MetricCollector {
					collector := mocks.NewMockMetricCollector(ctrl)
					collector.EXPECT().QueryDurationObserve(
						"postgres",
						"127.0.0.1:5432",
						"postgresdb",
						"tx.commit",
						"",
						true,
						gomock.Any(),
					)

					return collector
				},
			},
			do: func(hook *MetricsHook) {
				input := &dbhook.HookInput{
					Query:  "COMMIT",
					Caller: dbhook.CallerCommit,
				}

				ctx, _ = hook.Before(ctx, input)

				input.Error = errors.New("some error")

				ctx, _ = hook.Error(ctx, input)
			},
		},
		{
			name: "serialization failure",
			args: args{
				config: &Config{
					Addr:     "127.0.0.1:5432",
					User:     "test",
					Database: "postgresdb",
					Type:     "postgres",
				},
				makeCollector: func() MetricCollector {
					collector := mocks.NewMockMetricCollector(ctrl)
					collector.EXPECT().QueryDurationObserve(
						"postgres",
						"127.0.0.1:5432",
						"postgresdb",
						"insert",
						"users",
						false,
						gomock.Any(),
					)
					collector.EXPECT().SerializationFailureInc(
						"postgres",
						"127.0.0.1:5432",
						"postgresdb",
					)

					return collector
				},
			},
			do: func(hook *MetricsHook) {
				input := &dbhook.HookInput{
					Query:  "INSERT INTO users (id) VALUES ($1)",
					Caller: dbhook.CallerQuery,
				}

				ctx, _ = hook.Before(ctx, input)

				input.Error = &pq.Error{Code: "40001"}

				ctx, _ = hook.Error(ctx, input)
			},
		},
		{
			name: "unknown query",
			args: args{
				config: &Config{
					Addr:     "127.0.0.1:5432",
					User:     "test",
					Database: "postgresdb",
					Type:     "postgres",
				},
				makeCollector: func() MetricCollector {
					collector := mocks.NewMockMetricCollector(ctrl)
					collector.EXPECT().QueryDurationObserve(
						"postgres",
						"127.0.0.1:5432",
						"postgresdb",
						"unknown",
						"unknown",
						true,
						gomock.Any(),
					)

					return collector
				},
			},
			do: func(hook *MetricsHook) {
				input := &dbhook.HookInput{
					Query:  "RANDOM TEXT",
					Caller: dbhook.CallerQuery,
				}

				ctx, _ = hook.Before(ctx, input)

				input.Error = errors.New("some error")

				ctx, _ = hook.Error(ctx, input)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hook := NewMetricsHook(tt.args.config, tt.args.makeCollector())
			tt.do(hook)
		})
	}
}
