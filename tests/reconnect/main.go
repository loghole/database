package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	"github.com/loghole/database"
	"github.com/loghole/database/hooks"
)

func main() {
	db, err := database.New(&database.Config{
		Addr: "haproxy:12757",
		User: "root",
		Type: database.PostgresDatabase,
	})
	if err != nil {
		panic(err)
	}

	initDatabase(db)

	testRetry(db)

	testReconnect(db)
}

func testRetry(db *database.DB) {
	err := db.RunTxx(context.Background(), func(ctx context.Context, tx *sqlx.Tx) error {
		return sql.ErrNoRows
	})

	if !errors.Is(err, sql.ErrNoRows) {
		panic("retry bad error")
	}
}

func testReconnect(db *database.DB) {
	var val string
	if err := db.GetContext(context.Background(), &val, `SELECT name FROM test.test LIMIT 1`); err != nil {
		panic(err)
	}

	time.Sleep(time.Second * 15) // nolint:gomnd // todo

	if err := db.GetContext(context.Background(), &val, `SELECT name FROM test.test LIMIT 1`); err != nil {
		if !errors.Is(err, hooks.ErrCanRetry) {
			panic(err)
		}
	} else {
		panic("no error")
	}

	if err := db.GetContext(context.Background(), &val, `SELECT name FROM test.test LIMIT 1`); err != nil {
		panic(err)
	}

	log.Println("reconnect work")
}

type testInsert struct {
	Name string `db:"name"`
}

func initDatabase(db *database.DB) {
	if _, err := db.ExecContext(context.TODO(), `CREATE DATABASE IF NOT EXISTS test`); err != nil {
		panic(err)
	}

	if _, err := db.ExecContext(context.TODO(), `CREATE TABLE IF NOT EXISTS test.test(
		id UUID NOT NULL DEFAULT gen_random_uuid(),
		name STRING NOT NULL
	)`); err != nil {
		panic(err)
	}

	if _, err := db.ExecContext(context.TODO(), `INSERT INTO test.test(name) VALUES('test')`); err != nil {
		panic(err)
	}

	if _, err := db.NamedExecContext(context.TODO(), `INSERT INTO test.test(name) VALUES(:name)`,
		&testInsert{Name: "asd"}); err != nil {
		panic(err)
	}
}
