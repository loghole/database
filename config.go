package db

import (
	"fmt"

	"github.com/loghole/db/hooks"
)

type DatabaseType string

const (
	PostgresDatabase   DatabaseType = "postgres"
	ClickhouseDatabase DatabaseType = "clickhouse"
	SQLiteDatabase     DatabaseType = "sqlite3"
)

func (d DatabaseType) String() string {
	return string(d)
}

type Config struct {
	Addr         string
	User         string
	Database     string
	CertPath     string
	Type         DatabaseType
	ReadTimeout  string
	WriteTimeout string
}

func (cfg *Config) dataSourceName() (connStr string) {
	switch cfg.Type {
	case PostgresDatabase:
		connStr = cfg.postgresConnString()
	case ClickhouseDatabase:
		connStr = cfg.clickhouseConnString()
	case SQLiteDatabase:
		connStr = cfg.sqliteConnString()
	}

	return connStr
}

func (cfg *Config) postgresConnString() string {
	switch {
	case cfg.CertPath != "":
		ssl := fmt.Sprintf("&sslmode=%s&sslcert=%s/client.%s.crt&sslkey=%s/client.%s.key&sslrootcert=%s/ca.crt",
			"verify-full", cfg.CertPath, cfg.User, cfg.CertPath, cfg.User, cfg.CertPath)

		return fmt.Sprintf("postgres://%s@%s/%s?%s", cfg.User, cfg.Addr, cfg.Database, ssl)
	default:
		return fmt.Sprintf("postgresql://%s@%s/%s?sslmode=disable", cfg.User, cfg.Addr, cfg.Database)
	}
}

func (cfg *Config) clickhouseConnString() string {
	return fmt.Sprintf("tcp://%s?username=%s&database=%s&read_timeout=%s&write_timeout=%s",
		cfg.Addr, cfg.User, cfg.Database, cfg.ReadTimeout, cfg.WriteTimeout)
}

func (cfg *Config) sqliteConnString() string {
	return cfg.Database
}

func (cfg *Config) driverName() string {
	return string(cfg.Type)
}

func (cfg *Config) hookConfig() *hooks.Config {
	return &hooks.Config{
		Addr:           cfg.Addr,
		User:           cfg.User,
		Database:       cfg.Database,
		CertPath:       cfg.CertPath,
		Type:           cfg.Type.String(),
		ReadTimeout:    cfg.ReadTimeout,
		WriteTimeout:   cfg.WriteTimeout,
		DataSourceName: cfg.dataSourceName(),
		DriverName:     cfg.driverName(),
		Instance:       "-",
	}
}
