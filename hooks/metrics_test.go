package hooks

import (
	"bufio"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/lib/pq"
	"github.com/loghole/dbhook"
	"github.com/stretchr/testify/assert"

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

				input.Error = pq.Error{Code: "40001"}

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

func TestScanSQLToken(t *testing.T) {
	type args struct {
		query string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "select",
			args: args{
				query: "SELECT id FROM users WHERE name=$1",
			},
			want: []string{"SELECT", "id", "FROM", "users", "WHERE", "name=$1"},
		},
		{
			name: "insert",
			args: args{
				query: "INSERT INTO users (id, name) VALUES ($1, $2)",
			},
			want: []string{"INSERT", "INTO", "users", "id", "name", "VALUES", "$1", "$2"},
		},
		{
			name: "insert short",
			args: args{
				query: "INSERT INTO users(id, name)VALUES($1, $2)",
			},
			want: []string{"INSERT", "INTO", "users", "id", "name", "VALUES", "$1", "$2"},
		},
		{
			name: "with insert",
			args: args{
				query: "WITH q1 AS(SELECT id, name FROM users)INSERT INTO users(id, name)VALUES(q1.id, q1.name) FROM q1",
			},
			want: []string{"WITH", "q1", "AS", "SELECT", "id", "name", "FROM", "users", "INSERT", "INTO", "users", "id", "name", "VALUES", "q1", "id", "q1", "name", "FROM", "q1"},
		},
		{
			name: "specific delimiters",
			args: args{
				query: "WITH" + string('\u0085') + "q1" + string('\u1680') + "AS(SELECT id, name FROM users)INSERT" + string('\u2000') + "INTO users(id, name)VALUES(q1.id, q1.name) FROM q1",
			},
			want: []string{"WITH", "q1", "AS", "SELECT", "id", "name", "FROM", "users", "INSERT", "INTO", "users", "id", "name", "VALUES", "q1", "id", "q1", "name", "FROM", "q1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scan := bufio.NewScanner(strings.NewReader(tt.args.query))
			scan.Split(scanSQLToken)

			var result []string

			for scan.Scan() {
				result = append(result, scan.Text())
			}

			if !assert.NoError(t, scan.Err()) {
				return
			}

			assert.Equalf(t, tt.want, result, "result not equal")
		})
	}
}
