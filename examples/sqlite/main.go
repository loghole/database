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
