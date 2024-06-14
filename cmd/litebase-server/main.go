package main

import (
	"litebase/server"
	"log"

	"github.com/joho/godotenv"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	godotenv.Load(".env")

	server.NewServer().Start(func(s *server.ServerInstance) {
		server.NewApp(s).Run()
	})
}
