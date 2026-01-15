package server

import (
	"log/slog"
	"time"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/http"
	"github.com/litebase/litebase/pkg/logs"
	"github.com/litebase/litebase/pkg/scheduler"
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

	err = auth.InitKey(app.Config, clusterInstance.ObjectFS())

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

	app.Cluster.Node().Init(
		database.NewQueryBuilder(app.Cluster, app.Auth, app.DatabaseManager, app.LogManager),
		database.ResponsePool(),
		database.NewDatabaseWALSynchronizer(app.DatabaseManager),
	)

	// Set the page logger accessor for distributed page compaction coordination
	app.Cluster.Node().SetPageLoggerAccessor(
		database.NewDatabasePageLoggerAccessor(app.DatabaseManager),
	)

	// Set the database manager for handling database branch settings updates
	app.Cluster.Node().SetDatabaseManager(
		database.NewDatabaseManagerAdapter(app.DatabaseManager),
	)

	app.Cluster.EventsManager().Init()
	app.Auth.Broadcaster(app.Cluster.EventsManager().Hook())

	app.Cluster.Node().OnStarted(func() {
		app.Auth.ProvideAccessKeyStorage(
			database.NewSystemDatabaseAccessKeyStorage(
				app.Config,
				app.Auth.SecretsManager,
				app.DatabaseManager.SystemDatabase(),
			),
		)

		app.Auth.ProvideTokenStorage(
			database.NewSystemDatabaseTokenStorage(
				app.Config,
				app.Auth.SecretsManager,
				app.DatabaseManager.SystemDatabase(),
			),
		)

		app.Auth.ProvideUserManagerStorage(
			database.NewSystemDatabaseUserStorage(
				app.DatabaseManager.SystemDatabase(),
			),
		)

		err = app.Auth.UserManager.Init()

		if err != nil {
			slog.Error("Error initializing user manager", "error", err)
		}
	})

	// Initialize the queue worker pool first
	app.QueueWorkerPool = queue.NewWorkerPool(
		app.DatabaseManager.SystemDatabase(),
		app.Cluster,
		queue.WorkerPoolConfig{
			PrimaryOnly: true, // Only primary nodes process jobs
		},
	)

	// Create dispatcher from the worker pool so it uses the same job registry
	app.QueueDispatcher = app.QueueWorkerPool.NewDispatcher()

	app.InitQueueJobs()

	// Initialize the scheduler
	app.Scheduler = scheduler.NewScheduler(
		app.DatabaseManager.SystemDatabase(),
		app.Cluster.Node().IsPrimary,
	)

	app.InitScheduledTasks()

	// Start worker pool and scheduler on all nodes when node starts
	// Primary check happens during execution
	app.Cluster.Node().OnStarted(func() {
		// Record primary node startup timestamp and perform gap analysis (primary only)
		if app.Cluster.Node().IsPrimary() {
			db, err := app.DatabaseManager.SystemDatabase().DB()

			if err == nil {
				now := time.Now().UTC().Format(time.RFC3339)

				_, err = db.Exec("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
					"primary_node_started_at", now)

				if err != nil {
					slog.Error("Failed to record primary node startup timestamp", "error", err)
				}

				_, err = db.Exec("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
					"primary_node_id", app.Cluster.Node().ID)

				if err != nil {
					slog.Error("Failed to record primary node ID", "error", err)
				}
			}

			// Perform gap analysis before starting scheduler (primary only)
			gaps, err := app.Scheduler.AnalyzeGaps()

			if err != nil {
				slog.Error("Failed to analyze gaps in scheduled tasks", "error", err)
			} else if len(gaps) > 0 {
				// Execute missed critical tasks
				app.Scheduler.ExecuteMissedTasks(gaps)
			}
		}

		// Start worker pool and scheduler on ALL nodes
		// Each job/task checks isPrimary() before executing
		if err := app.QueueWorkerPool.Start(); err != nil {
			slog.Error("Failed to start queue worker pool", "error", err)
		}

		app.Scheduler.Start()
	})

	go app.DatabaseManager.WriteQueueManager.Run()
	go app.LogManager.Run()

	app.initialized = true

	return app
}

func (app *App) IsInitialized() bool {
	return app.initialized
}

func (app *App) Shutdown() {
	// Record shutdown timestamp if this is the primary node
	if app.Cluster.Node().IsPrimary() {
		if app.DatabaseManager.SystemDatabase() != nil {
			db, err := app.DatabaseManager.SystemDatabase().DB()
			if err == nil {
				now := time.Now().UTC().Format(time.RFC3339)

				_, err = db.Exec("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
					"primary_node_stopped_at", now)

				if err != nil {
					slog.Error("Failed to record primary node shutdown timestamp", "error", err)
				} else {
					slog.Info("Recorded primary node shutdown", "timestamp", now)
				}
			}
		}
	}

	if err := app.Scheduler.Stop(); err != nil {
		slog.Error("Failed to stop scheduler", "error", err)
	}

	app.QueueWorkerPool.Stop()
}

func (app *App) Run() {
	http.NewRouter().Server(app.Cluster, app.DatabaseManager, app.LogManager, app.ServeMux)
}
