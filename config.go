package database

import (
	"fmt"

	"github.com/loghole/database/hooks"
	"github.com/loghole/database/internal/addrlist"
	"github.com/loghole/database/internal/pool"
)

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

type Config struct {
	Addr         string
	AddrList     *addrlist.AddrList // optional for cockroach.
	User         string
	Database     string
	CertPath     string
	Type         DBType
	ReadTimeout  string
	WriteTimeout string
}

func (cfg *Config) AddAddr(priority, weight uint, addrs ...string) error {

	return nil
}

func (cfg *Config) GetAddrList() *addrlist.AddrList {
	for _, addr := range cfg.AddrList.All() {
		addr.Addr = postgresConnString(cfg.User, addr.Addr, cfg.Database, cfg.CertPath)
	}

	return cfg.AddrList
}

func (cfg *Config) nodeConfigs() []pool.DBNodeConfig {
	configs := make([]pool.DBNodeConfig, 0)

	for _, addr := range cfg.AddrList.All() {
		config := pool.DBNodeConfig{
			Addr:       postgresConnString(cfg.User, addr.Addr, cfg.Database, cfg.CertPath),
			DriverName: cfg.driverName(),
			Priority:   addr.Priority,
			Weight:     addr.Weight,
		}

		configs = append(configs, config)
	}

	return configs
}

func (cfg *Config) DSN() (connStr string) {
	return cfg.dataSourceName()
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
		return fmt.Sprintf("postgres://%s@%s/%s?sslmode=disable", cfg.User, cfg.Addr, cfg.Database)
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

func postgresConnString(user, addr, database, certPath string) string {
	switch {
	case certPath != "":
		ssl := fmt.Sprintf("&sslmode=%s&sslcert=%s/client.%s.crt&sslkey=%s/client.%s.key&sslrootcert=%s/ca.crt",
			"verify-full", certPath, user, certPath, user, certPath)

		return fmt.Sprintf("postgres://%s@%s/%s?%s", user, addr, database, ssl)
	default:
		return fmt.Sprintf("postgres://%s@%s/%s?sslmode=disable", user, addr, database)
	}
}
