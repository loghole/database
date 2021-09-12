package database

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/loghole/database/internal/pool"
)

var (
	ErrInvalidConfig     = errors.New("invalid config")
	ErrAddrAlreadyExists = errors.New("addr alreay exists")
)

type DBType = pool.DBType

const (
	CockroachDatabase  DBType = pool.CockroachDatabase
	PostgresDatabase   DBType = pool.PostgresDatabase
	ClickhouseDatabase DBType = pool.ClickhouseDatabase
	SQLiteDatabase     DBType = pool.SQLiteDatabase
)

type Config struct {
	// The Addr can contain priority and weight semantics.
	// e.g. 127.0.0.1:8080?priority=1&weight=10&read_timeout=5s&write_timeout=5s
	// Comma separated arrays are also supported, e.g. url1, url2.
	Addr     string
	User     string
	Database string
	CertPath string
	Type     DBType

	ActiveCount      int
	CanUseOtherLevel bool
}

func (cfg *Config) paseAddr(addr string) (*pool.DBNodeConfig, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, fmt.Errorf("parse db addr: '%s', %w", addr, err)
	}

	config := &pool.DBNodeConfig{
		Addr:         u.Host,
		User:         cfg.User,
		Database:     cfg.Database,
		CertPath:     cfg.CertPath,
		Type:         cfg.Type,
		ReadTimeout:  u.Query().Get("read_timeout"),
		WriteTimeout: u.Query().Get("write_timeout"),
	}

	const (
		base    = 10
		bitSize = 32
	)

	if val := u.Query().Get("weight"); val != "" {
		weight, err := strconv.ParseUint(u.Query().Get("weight"), base, bitSize)
		if err != nil {
			return nil, fmt.Errorf("parse weight value '%s': %w", val, err)
		}

		config.Weight = uint(weight)
	}

	if val := u.Query().Get("priority"); val != "" {
		priority, err := strconv.ParseUint(u.Query().Get("priority"), base, bitSize)
		if err != nil {
			return nil, fmt.Errorf("parse priority value '%s': %w", val, err)
		}

		config.Priority = uint(priority)
	}

	return config, nil
}

func (cfg *Config) buildNodeConfigs() ([]*pool.DBNodeConfig, error) {
	switch {
	case cfg.Type.DriverName() == "":
		return nil, fmt.Errorf("required driver value %w", ErrInvalidConfig)
	case cfg.User == "":
		return nil, fmt.Errorf("required user: %w", ErrInvalidConfig)
	}

	var (
		addrs   = strings.Split(cfg.Addr, ",")
		configs = make([]*pool.DBNodeConfig, 0, len(addrs))
	)

	if len(addrs) > 1 && cfg.Type != pool.CockroachDatabase {
		return nil, fmt.Errorf("%w: multiple databases not supported for %s", ErrInvalidConfig, cfg.Type)
	}

	for _, addr := range addrs {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}

		config, err := cfg.paseAddr(addr)
		if err != nil {
			return nil, fmt.Errorf("parse addr: %w", err)
		}

		for _, target := range configs {
			if strings.EqualFold(config.Addr, target.Addr) {
				return nil, fmt.Errorf("%s: %w", config.Addr, ErrAddrAlreadyExists)
			}
		}

		configs = append(configs, config)
	}

	return configs, nil
}

func (cfg *Config) DSN() (string, error) {
	configs, err := cfg.buildNodeConfigs()
	if err != nil {
		return "", fmt.Errorf("build node configs: %w", err)
	}

	if len(configs) == 0 {
		return "", fmt.Errorf("empty addr: %w", ErrInvalidConfig)
	}

	return configs[0].DSN(), nil
}
