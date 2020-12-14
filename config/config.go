package config

import (
	"fmt"
)

type DBType string

const (
	DBTypePostgres   DBType = "postgres"
	DBTypeClickhouse DBType = "clickhouse"
	DBTypeSQLite     DBType = "sqlite3"
)

type Config struct {
	Addr         string
	User         string
	Database     string
	CertPath     string
	Type         DBType
	ReadTimeout  string
	WriteTimeout string
}

func (cfg *Config) DataSourceName() (connStr string) {
	switch cfg.Type {
	case DBTypePostgres:
		connStr = cfg.PostgresConnString()
	case DBTypeClickhouse:
		connStr = cfg.ClickhouseConnString()
	case DBTypeSQLite:
		connStr = cfg.SQLiteConnString()
	}

	return connStr
}

func (cfg *Config) PostgresConnString() string {
	switch {
	case cfg.CertPath != "":
		ssl := fmt.Sprintf("&sslmode=%s&sslcert=%s/client.%s.crt&sslkey=%s/client.%s.key&sslrootcert=%s/ca.crt",
			"verify-full", cfg.CertPath, cfg.User, cfg.CertPath, cfg.User, cfg.CertPath)

		return fmt.Sprintf("postgres://%s@%s/%s?%s", cfg.User, cfg.Addr, cfg.Database, ssl)
	default:
		return fmt.Sprintf("postgresql://%s@%s/%s?sslmode=disable", cfg.User, cfg.Addr, cfg.Database)
	}
}

func (cfg *Config) ClickhouseConnString() string {
	return fmt.Sprintf("tcp://%s?username=%s&database=%s&read_timeout=%s&write_timeout=%s",
		cfg.Addr, cfg.User, cfg.Database, cfg.ReadTimeout, cfg.WriteTimeout)
}

func (cfg *Config) SQLiteConnString() string {
	return cfg.Database
}

func (cfg *Config) DriverName() string {
	return string(cfg.Type)
}
