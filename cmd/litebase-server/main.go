package main

import (
	"litebase/server"
	"log"

	"github.com/joho/godotenv"

	"net/http"
	_ "net/http/pprof"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	godotenv.Load(".env")

	server.NewServer().Start(func(s *server.ServerInstance) {
		server.NewApp(s).Run()
	})
}
