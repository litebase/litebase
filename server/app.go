package server

import (
	"litebase/internal/config"
	"litebase/server/auth"
	"litebase/server/cluster"
	"litebase/server/database"
	"litebase/server/http"
	"litebase/server/storage"

	netHttp "net/http"
)

type App struct {
	initialized     bool
	Auth            *auth.Auth
	DatabaseManager *database.DatabaseManager
	Cluster         *cluster.Cluster
	Config          *config.Config
	ServeMux        *netHttp.ServeMux
}

func attemptSecretInitialization(c *config.Config) bool {
	if c.Env == config.EnvTest {
		return true
	}

	return c.NodeType == config.NodeTypeQuery
}

func NewApp(configInstance *config.Config, serveMux *netHttp.ServeMux) *App {
	clusterInstance, err := cluster.NewCluster(configInstance)

	if err != nil {
		panic(err)
	}

	Auth := auth.NewAuth(
		configInstance,
		clusterInstance.ObjectFS(),
		clusterInstance.TmpFS(),
	)

	app := &App{
		Auth:            Auth,
		Cluster:         clusterInstance,
		Config:          configInstance,
		DatabaseManager: database.NewDatabaseManager(clusterInstance, Auth.SecretsManager),
		ServeMux:        serveMux,
	}

	storage.Init(
		app.Config,
		app.Cluster.ObjectFS(),
		app.Cluster.Node().Address(),
		app.Auth.SecretsManager,
	)

	err = clusterInstance.Init(Auth)

	if err != nil {
		panic(err)
	}

	if attemptSecretInitialization(app.Config) {
		err := auth.InitSignature(app.Config, clusterInstance.ObjectFS())

		if err != nil {
			panic(err)
		}
	}

	storage.SetDiscoveryProvider(app.Cluster)

	if attemptSecretInitialization(app.Config) {
		err = auth.KeyManagerInit(
			configInstance,
			app.Auth.SecretsManager,
		)

		if err != nil {
			panic(err)
		}
	}

	app.Auth.SecretsManager.Init()
	app.Auth.UserManager().Init()

	app.Cluster.Node().Init(
		database.NewQueryBuilder(app.Cluster, app.Auth.AccessKeyManager, app.DatabaseManager),
		database.NewDatabaseWalSynchronizer(app.DatabaseManager),
	)
	app.Cluster.EventsManager().Init()

	auth.Broadcaster(app.Cluster.EventsManager().Hook())
	storage.SetStorageContext(app.Cluster.Node().Context())
	storage.SetStorageTimestamp(app.Cluster.Node().Timestamp())

	go app.DatabaseManager.WriteQueueManager.Run()

	app.initialized = true

	return app
}

func (app *App) IsInitialized() bool {
	return app.initialized
}

func (app *App) Run() {
	http.Router().Server(app.Cluster, app.DatabaseManager, app.ServeMux)
}
