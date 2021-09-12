package main

import (
	"context"
	"log"
	"time"

	"github.com/loghole/database"
)

func main() {
	config := &database.Config{
		Addr: `
			cockroach://127.0.0.1:26257?priority=1&weight=1,
			cockroach://127.0.0.1:26258?priority=1&weight=10,
			cockroach://127.0.0.1:26259?priority=2&weight=1,
			cockroach://127.0.0.1:26260?priority=2&weight=10,
			cockroach://127.0.0.1:26261?priority=3&weight=1,
		`,
		User:        "root",
		Type:        database.CockroachDatabase,
		ActiveCount: 2,
	}

	db, err := database.New(config)
	if err != nil {
		panic(err)
	}

	for range time.NewTicker(time.Millisecond * 200).C {
		var dest []string

		if err := db.SelectContext(context.TODO(), &dest, "SHOW node_id"); err != nil {
			log.Println("SELECT NODE ID: ", err)
		}

		log.Println("SELECT NODE ID: ", dest)
	}
}
