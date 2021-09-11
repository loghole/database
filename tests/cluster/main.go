package main

import (
	"github.com/loghole/database"
	"github.com/loghole/database/internal/addrlist"
)

func main() {
	var addrList addrlist.AddrList

	addrList.Add(1, 1, "postgres", "")

	db, err := database.NewDB2(&database.Config{
		AddrList: addrList,
		User:     "root",
		Type:     database.CockroachDatabase,
	})
	if err != nil {
		panic(err)
	}
}
