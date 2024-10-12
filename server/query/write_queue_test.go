package query_test

import (
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/query"
	"testing"
)

func BenchmarkWriteQueue(b *testing.B) {
	test.Run(b, func(app *server.App) {
		mock := test.MockDatabase(app)
		wq := query.GetWriteQueue(&query.Query{
			DatabaseKey: mock.DatabaseKey,
		})

		for i := 0; i < b.N; i++ {
			for j := 0; j < 100000; j++ {
				wq.Handle(func(f func(query *query.Query, response *query.QueryResponse) error, query *query.Query, response *query.QueryResponse) error {
					return nil
				}, func(query *query.Query, response *query.QueryResponse) error {
					return nil
				}, &query.Query{}, &query.QueryResponse{})
			}
		}
	})

}
