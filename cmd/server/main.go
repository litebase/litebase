package main

import (
	"log"

	"litebasedb/server"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	server.NewServer().Start(func(s *server.ServerInstance) {
		server.NewApp(s).Run()
	})
}
