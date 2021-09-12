package pool

import (
	"context"
	"fmt"
	"log"
)

type ctxkey string

const dbConfigKey ctxkey = "db-node-config"

type DBType string

const (
	CockroachDatabase  DBType = "cockroach"
	PostgresDatabase   DBType = "postgres"
	ClickhouseDatabase DBType = "clickhouse"
	SQLiteDatabase     DBType = "sqlite3"
)

func (d DBType) String() string {
	return string(d)
}

func (d DBType) DriverName() string {
	switch d {
	case CockroachDatabase, PostgresDatabase:
		return PostgresDatabase.String()
	case ClickhouseDatabase:
		return ClickhouseDatabase.String()
	case SQLiteDatabase:
		return SQLiteDatabase.String()
	default:
		return ""
	}
}

func DBNodeConfigToContext(ctx context.Context, cfg *DBNodeConfig) context.Context {
	return context.WithValue(ctx, dbConfigKey, cfg)
}

func DBNodeConfigFromContext(ctx context.Context) (*DBNodeConfig, bool) {
	cfg, ok := ctx.Value(dbConfigKey).(*DBNodeConfig)

	return cfg, ok
}

type DBNodeConfig struct {
	Addr         string
	User         string
	Database     string
	CertPath     string
	Type         DBType
	ReadTimeout  string
	WriteTimeout string

	Priority uint
	Weight   uint

	NodeID string
}

func (cfg *DBNodeConfig) DSN() (connStr string) {
	log.Println("u1", cfg.User, cfg.Type, cfg.Addr)

	switch cfg.Type {
	case PostgresDatabase, CockroachDatabase:
		connStr = cfg.postgresConnString()
	case ClickhouseDatabase:
		connStr = cfg.clickhouseConnString()
	case SQLiteDatabase:
		connStr = cfg.sqliteConnString()
	}

	return connStr
}

func (cfg *DBNodeConfig) postgresConnString() string {
	switch {
	case cfg.CertPath != "":
		ssl := fmt.Sprintf("&sslmode=%s&sslcert=%s/client.%s.crt&sslkey=%s/client.%s.key&sslrootcert=%s/ca.crt",
			"verify-full", cfg.CertPath, cfg.User, cfg.CertPath, cfg.User, cfg.CertPath)

		return fmt.Sprintf("postgres://%s@%s/%s?%s", cfg.User, cfg.Addr, cfg.Database, ssl)
	default:
		return fmt.Sprintf("postgres://%s@%s/%s?sslmode=disable", cfg.User, cfg.Addr, cfg.Database)
	}
}

func (cfg *DBNodeConfig) clickhouseConnString() string {
	return fmt.Sprintf("tcp://%s?username=%s&database=%s&read_timeout=%s&write_timeout=%s",
		cfg.Addr, cfg.User, cfg.Database, cfg.ReadTimeout, cfg.WriteTimeout)
}

func (cfg *DBNodeConfig) sqliteConnString() string {
	return cfg.Database
}

func (cfg *DBNodeConfig) driverName() string {
	switch cfg.Type {
	case CockroachDatabase, PostgresDatabase:
		return PostgresDatabase.String()
	case ClickhouseDatabase:
		return ClickhouseDatabase.String()
	case SQLiteDatabase:
		return SQLiteDatabase.String()
	default:
		return ""
	}
}
