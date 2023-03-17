package main

import (
	"litebasedb/server"
	"log"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	server.NewServer().Start()
}
