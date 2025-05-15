package main

import (
	"log"
	"runtime"

	"github.com/litebase/litebase/common/config"
	"github.com/litebase/litebase/server"

	"github.com/joho/godotenv"

	"net/http"
	_ "net/http/pprof"
)

var app *server.App

func main() {
	go func() {
		runtime.SetBlockProfileRate(1)
		runtime.SetMutexProfileFraction(1)
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	godotenv.Load(".env")

	configInstance := config.NewConfig()

	server.NewServer(configInstance).Start(func(s *http.ServeMux) {
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

			// Shutdown all connections
			app.DatabaseManager.ConnectionManager().Shutdown()

			app.Cluster.Node().Shutdown()
		},
	)
}
