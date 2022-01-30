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
			want: "postgresql://postgres@127.0.0.1:5432/database?sslmode=disable",
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
			want: "postgres://postgres@127.0.0.1:5432/database?&sslmode=verify-full&sslcert=/certs/client.postgres.crt&sslkey=/certs/client.postgres.key&sslrootcert=/certs/ca.crt",
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
			want: "tcp://127.0.0.1:9000?username=default&database=database&read_timeout=10s&write_timeout=15s",
		},
		{
			name: "sqlite",
			config: &Config{
				Database: ":memory:",
				Type:     SQLiteDatabase,
			},
			want: ":memory:",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.config.DSN(), "DSN()")
		})
	}
}
