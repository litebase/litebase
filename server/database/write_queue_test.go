package database_test

import (
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/database"
	"testing"
)

func BenchmarkWriteQueue(b *testing.B) {
	test.RunWithApp(b, func(app *server.App) {
		mock := test.MockDatabase(app)

		wq := app.DatabaseManager.WriteQueueManager.GetWriteQueue(&database.Query{
			DatabaseKey: mock.DatabaseKey,
		})

		for i := 0; i < b.N; i++ {
			for j := 0; j < 100000; j++ {
				wq.Handle(func(f func(query *database.Query, response *database.QueryResponse) error, query *database.Query, response *database.QueryResponse) error {
					return nil
				}, func(query *database.Query, response *database.QueryResponse) error {
					return nil
				}, &database.Query{}, &database.QueryResponse{})
			}
		}
	})

}
