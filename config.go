package database

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"

	"github.com/loghole/database/hooks"
)

type DBType string

const (
	PGXDatabase        DBType = "pgx"
	PostgresDatabase   DBType = "postgres"
	CockroachDatabase  DBType = "cockroach"
	ClickhouseDatabase DBType = "clickhouse"
	SQLiteDatabase     DBType = "sqlite3"
)

func (d DBType) String() string {
	if d == CockroachDatabase {
		return string(PostgresDatabase)
	}

	return string(d)
}

type Config struct {
	Addr         string
	Addrs        []string // for cockroachdb
	User         string
	Database     string
	Type         DBType
	ReadTimeout  string
	WriteTimeout string
	Params       map[string]string

	// Deprecated: use Params for sets certs.
	CertPath string
}

func (cfg *Config) DSN() string {
	switch cfg.Type {
	case PostgresDatabase, PGXDatabase, CockroachDatabase:
		return cfg.postgresConnString()
	case ClickhouseDatabase:
		return cfg.clickhouseConnString()
	case SQLiteDatabase:
		return cfg.sqliteConnString()
	default:
		return ""
	}
}

func (cfg *Config) postgresConnString() string {
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}

	if cfg.CertPath != "" {
		cfg.Params["sslmode"] = "verify-full"
		cfg.Params["sslcert"] = "/certs/client.postgres.crt"
		cfg.Params["sslkey"] = "/certs/client.postgres.key"
		cfg.Params["sslrootcert"] = "/certs/ca.crt"
	}

	if len(cfg.Params) == 0 {
		cfg.Params["sslmode"] = "disable"
	}

	addr := cfg.Addr

	if len(cfg.Addrs) > 0 {
		addr = cfg.Addrs[rand.Intn(len(cfg.Addrs))] //nolint:gosec // not a security issue.
	}

	return fmt.Sprintf("postgres://%s@%s/%s%s", cfg.User, addr, cfg.Database, cfg.encodeParams())
}

func (cfg *Config) clickhouseConnString() string {
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}

	userParts := strings.Split(cfg.User, ":")

	if len(userParts) > 1 {
		cfg.Params["password"] = userParts[1]
	}

	cfg.Params["username"] = userParts[0]

	if cfg.ReadTimeout != "" {
		cfg.Params["read_timeout"] = cfg.ReadTimeout
	}

	if cfg.WriteTimeout != "" {
		cfg.Params["write_timeout"] = cfg.WriteTimeout
	}

	return fmt.Sprintf("clickhouse://%s/%s%s", cfg.Addr, cfg.Database, cfg.encodeParams())
}

func (cfg *Config) sqliteConnString() string {
	return fmt.Sprintf("%s%s", cfg.Database, cfg.encodeParams())
}

func (cfg *Config) driverName() string {
	return cfg.Type.String()
}

func (cfg *Config) hookConfig() *hooks.Config {
	return &hooks.Config{
		Addr:           cfg.Addr,
		User:           strings.Split(cfg.User, ":")[0], // trim password part if exists.
		Database:       cfg.Database,
		CertPath:       cfg.CertPath,
		Type:           cfg.Type.String(),
		ReadTimeout:    cfg.ReadTimeout,
		WriteTimeout:   cfg.WriteTimeout,
		DataSourceName: cfg.DSN(),
		DriverName:     cfg.driverName(),
		Instance:       "-",
	}
}

func (cfg *Config) encodeParams() string {
	if len(cfg.Params) == 0 {
		return ""
	}

	var buf strings.Builder

	keys := make([]string, 0, len(cfg.Params))

	for key := range cfg.Params {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	for _, key := range keys {
		val := cfg.Params[key]

		if buf.Len() > 0 {
			buf.WriteByte('&')
		}

		buf.WriteString(key)
		buf.WriteByte('=')
		buf.WriteString(val)
	}

	return "?" + buf.String()
}
