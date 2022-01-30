# Database
[![GoDoc](https://pkg.go.dev/badge/github.com/loghole/database)](https://pkg.go.dev/github.com/loghole/database)
[![Go Report Card](https://goreportcard.com/badge/github.com/loghole/database)](https://goreportcard.com/report/github.com/loghole/database)
[![Coverage Status](https://coveralls.io/repos/github/loghole/database/badge.svg)](https://coveralls.io/github/loghole/database)

Database is wrapper for [sqlx](https://github.com/jmoiron/sqlx) with clear interface, retry func and hooks.  
Compatible databases: PostgreSQL, Clickhouse, CockroachDB, SQLite.  

- [Database](#database)
- [Install](#install)
- [Usage](#usage)
- [Custom hooks](#custom-hooks)
# Install
```sh
go get github.com/loghole/database
```

# Usage
```go
package main

import (
	"context"
	"log"

	"github.com/loghole/database"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	// Make connection config
	cfg := &database.Config{
		Database: ":memory:",
		Type:     database.SQLiteDatabase,
	}

	// Connect to Database with hooks
	db, err := database.New(cfg, database.WithSimplerrHook(), database.WithRetryFunc(retry))
	if err != nil {
		panic(err)
	}

	defer db.Close()

	ctx := context.Background()

	// Queries
	db.ExecContext(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, text VARCHAR(5))")
	db.ExecContext(ctx, "INSERT into t (id, text) VALUES(?, ?)", 1, "foo")
	// Try 3 times and return error
	if _, err := db.ExecContext(ctx, "INSERT into t (id, text) VALUES(?, ?)", 1, "bar"); err != nil {
		log.Println(err)
	}
}

func retry(retryCount int, err error) bool {
	if retryCount > 3 {
		return false
	}

	log.Println(retryCount, err)

	return true
}
```

# Custom hooks
You can write custom hooks with [dbhook](https://github.com/loghole/dbhook) and use options `database.WithCustomHook(hook)`
