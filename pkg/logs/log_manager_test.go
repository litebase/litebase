package logs_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/logs"
	"github.com/litebase/litebase/server"
)

func TestNewLogManager(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		logManager := logs.NewLogManager(app.Cluster.Node().Context())

		if logManager == nil {
			t.Fatal("Log manager is nil")
		}
	})
}

func TestLogManager_GetQueryLog(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		l := app.LogManager.GetQueryLog(
			app.Cluster,
			db.DatabaseKey.DatabaseHash,
			db.DatabaseId,
			db.BranchId,
		)

		if l == nil {
			t.Fatal("Query log is nil")
		}
	})
}

func TestLogManager_Close(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		err := app.LogManager.Close()

		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestLogManager_Query(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		err := app.LogManager.Query(
			logs.QueryLogEntry{
				Cluster:      app.Cluster,
				DatabaseHash: db.DatabaseKey.DatabaseHash,
				DatabaseId:   db.DatabaseId,
				BranchId:     db.BranchId,
				AccessKeyId:  db.AccessKey.AccessKeyId,
				Statement:    []byte("SELECT * FROM test"),
				Latency:      0.01,
			},
		)

		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestLogManager_Run(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		go app.LogManager.Run()

		app.Cluster.Node().Shutdown()
	})
}
