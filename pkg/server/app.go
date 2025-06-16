package server

import (
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/http"
	"github.com/litebase/litebase/pkg/logs"
	"github.com/litebase/litebase/pkg/storage"

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

	app := &App{
		Cluster:  clusterInstance,
		Config:   configInstance,
		ServeMux: serveMux,
	}

	storage.Init(
		app.Config,
		app.Cluster.ObjectFS(),
	)

	app.Auth = auth.NewAuth(
		configInstance,
		clusterInstance.NetworkFS(),
		clusterInstance.ObjectFS(),
		clusterInstance.TmpFS(),
		clusterInstance.TmpTieredFS(),
	)
	app.DatabaseManager = database.NewDatabaseManager(clusterInstance, app.Auth.SecretsManager)
	app.LogManager = logs.NewLogManager(app.Cluster.Node().Context())

	err = clusterInstance.Init(app.Auth)

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
	app.Auth.Broadcaster(app.Cluster.EventsManager().Hook())

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
