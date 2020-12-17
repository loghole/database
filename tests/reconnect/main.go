package main

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/loghole/db"
	"github.com/loghole/db/hooks"

	_ "github.com/lib/pq"
)

func main() {
	datababase, err := db.New(&db.Config{
		Addr: "haproxy:12757",
		User: "root",
		Type: db.PostgresDatabase,
	}, db.WithReconnectHook())
	if err != nil {
		panic(err)
	}

	initDatabase(datababase)

	var val string
	if err := datababase.Get(&val, `SELECT name FROM test.test LIMIT 1`); err != nil {
		panic(err)
	}

	time.Sleep(time.Second * 15) // nolint:gomnd // todo

	if err = datababase.Get(&val, `SELECT name FROM test.test LIMIT 1`); err != nil {
		if !errors.Is(err, hooks.ErrCanRetry) {
			panic(err)
		}
	} else {
		panic("no error")
	}

	if err := datababase.Get(&val, `SELECT name FROM test.test LIMIT 1`); err != nil {
		panic(err)
	}

	log.Println("reconnect work")
}

func initDatabase(datababase *sqlx.DB) {
	if _, err := datababase.ExecContext(context.TODO(), `CREATE DATABASE IF NOT EXISTS test`); err != nil {
		panic(err)
	}

	if _, err := datababase.ExecContext(context.TODO(), `CREATE TABLE IF NOT EXISTS test.test(
		id UUID NOT NULL DEFAULT gen_random_uuid(),
		name STRING NOT NULL
	)`); err != nil {
		panic(err)
	}

	if _, err := datababase.ExecContext(context.TODO(), `INSERT INTO test.test(name) VALUES('test')`); err != nil {
		panic(err)
	}
}
