package database

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_DSN(t *testing.T) {
	tests := []struct {
		name   string
		config *Config
		want   string
	}{
		{
			name: "postgres",
			config: &Config{
				Addr:     "127.0.0.1:5432",
				User:     "postgres",
				Database: "database",
				Type:     PostgresDatabase,
			},
			want: "postgres://postgres@127.0.0.1:5432/database?sslmode=disable",
		},
		{
			name: "postgres with certs",
			config: &Config{
				Addr:     "127.0.0.1:5432",
				User:     "postgres",
				Database: "database",
				Type:     PostgresDatabase,
				CertPath: "/certs",
			},
			want: "postgres://postgres@127.0.0.1:5432/database?sslcert=/certs/client.postgres.crt&sslkey=/certs/client.postgres.key&sslmode=verify-full&sslrootcert=/certs/ca.crt",
		},
		{
			name: "postgres with params",
			config: &Config{
				Addr:     "127.0.0.1:5432",
				User:     "postgres",
				Database: "database",
				Type:     PostgresDatabase,
				Params: map[string]string{
					"sslmode":     "verify-full",
					"sslcert":     "/certs/client.postgres.crt",
					"sslkey":      "/certs/client.postgres.key",
					"sslrootcert": "/certs/ca.crt",
				},
			},
			want: "postgres://postgres@127.0.0.1:5432/database?sslcert=/certs/client.postgres.crt&sslkey=/certs/client.postgres.key&sslmode=verify-full&sslrootcert=/certs/ca.crt",
		},
		{
			name: "clickhouse",
			config: &Config{
				Addr:         "127.0.0.1:9000",
				User:         "default",
				Database:     "database",
				Type:         ClickhouseDatabase,
				ReadTimeout:  "10s",
				WriteTimeout: "15s",
			},
			want: "clickhouse://127.0.0.1:9000/database?read_timeout=10s&username=default&write_timeout=15s",
		},
		{
			name: "clickhouse without params",
			config: &Config{
				Addr:     "127.0.0.1:9000",
				User:     "default",
				Database: "database",
				Type:     ClickhouseDatabase,
			},
			want: "clickhouse://127.0.0.1:9000/database?username=default",
		},
		{
			name: "sqlite",
			config: &Config{
				Database: ":memory:",
				Type:     SQLiteDatabase,
			},
			want: ":memory:",
		},
		{
			name: "sqlite with params",
			config: &Config{
				Database: ":memory:",
				Type:     SQLiteDatabase,
				Params: map[string]string{
					"_auth_user": "user",
					"_auth_pass": "password",
				},
			},
			want: ":memory:?_auth_pass=password&_auth_user=user",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.config.DSN(), "DSN()")
		})
	}
}
