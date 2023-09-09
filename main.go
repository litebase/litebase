package main

import (
	"log"

	"litebasedb/app"
	"litebasedb/server"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	server.NewServer().Start(func(s *server.Server) {
		app.NewApp(s).Run()
	})
}
