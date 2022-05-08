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
	db, err := database.New(cfg, database.WithSimplerrHook(), database.WithRetryPolicy(database.RetryPolicy{
		MaxAttempts:       database.DefaultRetryAttempts,
		InitialBackoff:    database.DefaultRetryInitialBackoff,
		MaxBackoff:        database.DefaultRetryMaxBackoff,
		BackoffMultiplier: database.DefaultRetryBackoffMultiplier,
		ErrIsRetryable:    retry,
	}))
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

func retry(err error) bool {
	log.Println(err)

	return true
}
