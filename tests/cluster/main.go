package main

import (
	"context"
	"log"
	"time"

	"github.com/loghole/database"
	"github.com/loghole/database/internal/addrlist"
)

func main() {
	addrList := new(addrlist.AddrList)

	addrList.Add(1, 1, "127.0.0.1:26257", "127.0.0.1:26258")
	addrList.Add(2, 1, "127.0.0.1:26259", "127.0.0.1:26260")
	addrList.Add(3, 1, "127.0.0.1:26261")

	db, err := database.NewDB2(&database.Config{
		AddrList: addrList,
		User:     "root",
		Type:     database.CockroachDatabase,
	})
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
