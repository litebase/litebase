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

		for i := 0; i < b.N; i++ {
			for j := 0; j < 100000; j++ {
				wq.Handle(func(f func(query *database.Query, response *database.QueryResponse) (*database.QueryResponse, error), query *database.Query, response *database.QueryResponse) (*database.QueryResponse, error) {
					return nil, nil
				}, func(query *database.Query, response *database.QueryResponse) (*database.QueryResponse, error) {
					return nil, nil
				}, &database.Query{}, &database.QueryResponse{})
			}
		}
	})

}
