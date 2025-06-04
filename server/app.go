package server

import (
	"github.com/litebase/litebase/common/config"
	"github.com/litebase/litebase/server/auth"
	"github.com/litebase/litebase/server/cluster"
	"github.com/litebase/litebase/server/database"
	"github.com/litebase/litebase/server/http"
	"github.com/litebase/litebase/server/logs"
	"github.com/litebase/litebase/server/storage"

	netHttp "net/http"
)

type App struct {
	initialized     bool
	Auth            *auth.Auth
	Cluster         *cluster.Cluster
	Config          *config.Config
	DatabaseManager *database.DatabaseManager
	LogManager      *logs.LogManager
	ServeMux        *netHttp.ServeMux
}

func NewApp(configInstance *config.Config, serveMux *netHttp.ServeMux) *App {
	clusterInstance, err := cluster.NewCluster(configInstance)

	if err != nil {
		panic(err)
	}

	Auth := auth.NewAuth(
		configInstance,
		clusterInstance.NetworkFS(),
		clusterInstance.ObjectFS(),
		clusterInstance.TmpFS(),
		clusterInstance.TmpTieredFS(),
	)

	app := &App{
		Auth:            Auth,
		Cluster:         clusterInstance,
		Config:          configInstance,
		DatabaseManager: database.NewDatabaseManager(clusterInstance, Auth.SecretsManager),
		ServeMux:        serveMux,
	}

	app.LogManager = logs.NewLogManager(app.Cluster.Node().Context())
	address, _ := app.Cluster.Node().Address()

	storage.Init(
		app.Config,
		app.Cluster.ObjectFS(),
		address,
	)

	err = clusterInstance.Init(Auth)

	if err != nil {
		panic(err)
	}

	err = auth.InitSignature(app.Config, clusterInstance.ObjectFS())

	if err != nil {
		panic(err)
	}

	err = auth.KeyManagerInit(
		configInstance,
		app.Auth.SecretsManager,
	)

	if err != nil {
		panic(err)
	}

	err = app.Auth.SecretsManager.Init()

	if err != nil {
		panic(err)
	}

	app.Auth.UserManager().Init()

	app.Cluster.Node().Init(
		database.NewQueryBuilder(app.Cluster, app.Auth.AccessKeyManager, app.DatabaseManager, app.LogManager),
		database.ResponsePool(),
		database.NewDatabaseWALSynchronizer(app.DatabaseManager),
	)
	app.Cluster.EventsManager().Init()
	auth.Broadcaster(app.Cluster.EventsManager().Hook())

	go app.DatabaseManager.WriteQueueManager.Run()
	go app.LogManager.Run()

	app.initialized = true

	return app
}

func (app *App) IsInitialized() bool {
	return app.initialized
}

func (app *App) Run() {
	http.NewRouter().Server(app.Cluster, app.DatabaseManager, app.LogManager, app.ServeMux)
}
