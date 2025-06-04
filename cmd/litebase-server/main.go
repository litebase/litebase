package main

import (
	"log"
	"log/slog"
	"runtime"
	"time"

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

		start := app.Cluster.Node().Start()

		select {
		case <-start:
		case <-time.After(1 * time.Second):
			log.Fatal("Cluster node failed to start within 1 second")
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
