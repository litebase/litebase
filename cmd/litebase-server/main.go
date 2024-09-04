package main

import (
	"litebase/server"
	"log"
	"runtime"

	"github.com/joho/godotenv"

	"net/http"
	_ "net/http/pprof"
)

func main() {
	go func() {
		runtime.SetBlockProfileRate(1)

		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	godotenv.Load(".env")

	server.NewServer().Start(func(s *server.ServerInstance) {
		server.NewApp(s).Run()
	})
}
