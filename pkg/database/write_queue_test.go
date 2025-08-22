package database_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
)

func BenchmarkWriteQueue(b *testing.B) {
	test.RunWithApp(b, func(app *server.App) {
		mock := test.MockDatabase(app)

		wq := app.DatabaseManager.WriteQueueManager.GetWriteQueue(&database.Query{
			DatabaseKey: mock.DatabaseKey,
		})

		for b.Loop() {
			for range 100000 {
				_, err := wq.Handle(func(f func(query *database.Query, response *database.QueryResponse) (*database.QueryResponse, error), query *database.Query, response *database.QueryResponse) (*database.QueryResponse, error) {
					return nil, nil
				}, func(query *database.Query, response *database.QueryResponse) (*database.QueryResponse, error) {
					return nil, nil
				}, &database.Query{}, &database.QueryResponse{})

				if err != nil {
					b.Errorf("error handling write queue: %v", err)
				}
			}
		}
	})

}
