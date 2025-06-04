package main

import (
	"log"
	"log/slog"
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

	godotenv.Load(".env")

	configInstance := config.NewConfig()
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if configInstance.Debug {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	} else {
		slog.SetLogLoggerLevel(slog.LevelWarn)
	}

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
