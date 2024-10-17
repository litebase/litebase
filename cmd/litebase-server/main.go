package main

import (
	"litebase/internal/config"
	"litebase/server"
	"log"

	"github.com/joho/godotenv"

	"net/http"
	_ "net/http/pprof"
)

var app *server.App

func main() {
	go func() {
		// runtime.SetBlockProfileRate(1)

		// log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	godotenv.Load(".env")

	configInstance := config.NewConfig()

	server.NewServer(configInstance).Start(
		// Start hook
		func(s *http.ServeMux) {
			app = server.NewApp(configInstance, s)

			app.Run()

			err := app.Cluster.Node().Start()

			if err != nil {
				log.Fatalf("Node start: %v", err)
			}
		},
		// Shutdown hook
		func() {
			if app == nil {
				return
			}

			app.Cluster.Node().Shutdown()

			// Shutdown all connections
			app.DatabaseManager.ConnectionManager().Shutdown()
		},
	)
}
