package main

import (
	"log"
	"log/slog"
	"time"

	"github.com/litebase/litebase/pkg/config"
	httpRouter "github.com/litebase/litebase/pkg/http"
	"github.com/litebase/litebase/pkg/server"

	"github.com/joho/godotenv"

	netHttp "net/http"
	// _ "net/http/pprof"
)

func main() {
	// Debugging with pprof
	// Uncomment the following lines to enable pprof
	// go func() {
	// 	runtime.SetBlockProfileRate(1)
	// 	runtime.SetMutexProfileFraction(1)
	// 	log.Println(http.ListenAndServe("localhost:6060", nil))
	// }()

	err := godotenv.Load(".env")

	if err != nil {
		log.Fatal("Error loading .env file")
	}

	configInstance := config.NewConfig()

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if configInstance.Debug {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	} else {
		slog.SetLogLoggerLevel(slog.LevelInfo)
	}

	srv := server.NewServer(configInstance)

	srv.StartWithPrivateRouting(
		// Public server setup
		func(publicMux *netHttp.ServeMux, app *server.App) {
			app.Run()

			// Start the node
			start := app.Cluster.Node().Start()

			select {
			case <-start:
				// Node started successfully
			case <-time.After(1 * time.Second):
				log.Fatal("Cluster node failed to start within 1 second")
			}
		},
		// Private server setup
		func(privateMux *netHttp.ServeMux, app *server.App) {
			// Set up private routes
			router := httpRouter.NewRouter()
			router.PrivateServer(app.Cluster, app.DatabaseManager, app.LogManager, privateMux)
		},
		// Shutdown hook
		func(app *server.App) {
			// Shutdown all connections
			app.DatabaseManager.ConnectionManager().Shutdown()

			err = app.Cluster.Node().Shutdown()

			if err != nil {
				slog.Error("Failed to shutdown cluster node", "error", err)
			}
		},
	)
}
