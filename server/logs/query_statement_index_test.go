package logs_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server"
	"github.com/litebase/litebase/server/file"
	"github.com/litebase/litebase/server/logs"
)

func TestGetQueryStatementIndex(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		queryLogIndex, err := logs.GetQueryStatementIndex(
			app.Cluster.TieredFS(),
			fmt.Sprintf("%slogs/query", file.GetDatabaseFileBaseDir(db.DatabaseId, db.BranchId)),
			fmt.Sprintf("QUERY_STATEMENT_INDEX_%d", app.Cluster.Node().ID),
			0,
		)

		if err != nil {
			t.Fatalf("Error getting query statement index: %v", err)
		}

		if queryLogIndex == nil {
			t.Fatal("Query log index is nil")
		}
	})
}

func TestQueryStatementIndex_Close(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		queryLogIndex, err := logs.GetQueryStatementIndex(
			app.Cluster.TieredFS(),
			fmt.Sprintf("%slogs/query", file.GetDatabaseFileBaseDir(db.DatabaseId, db.BranchId)),
			fmt.Sprintf("QUERY_STATEMENT_INDEX_%s", app.Cluster.Node().Cluster.Id),
			0,
		)

		if err != nil {
			t.Fatalf("Error getting query statement index: %v", err)
		}

		err = queryLogIndex.Close()

		if err != nil {
			t.Fatalf("Error closing query statement index: %v", err)
		}
	})
}

func TestQueryStatementIndex_Get_Set(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		queryLogIndex, err := logs.GetQueryStatementIndex(
			app.Cluster.TieredFS(),
			fmt.Sprintf("%slogs/query", file.GetDatabaseFileBaseDir(db.DatabaseId, db.BranchId)),
			fmt.Sprintf("QUERY_STATEMENT_INDEX_%d", app.Cluster.Node().ID),
			0,
		)

		if err != nil {
			t.Fatalf("Error getting query statement index: %v", err)
		}

		value := fmt.Sprintf("access_key_id=%s statement=%s", "test", "SELECT * FROM test")

		err = queryLogIndex.Set("SELECT * FROM test", value)

		if err != nil {
			t.Fatalf("Error putting data in query statement index: %v", err)
		}

		data, found := queryLogIndex.Get("SELECT * FROM test")

		if !found {
			t.Fatal("Expected to find statement")
		}

		if data == nil {
			t.Fatal("Expected data to be not nil")
		}

		if string(data) != value {
			t.Fatalf("Expected data to be 1, got %v", data[0])
		}
	})
}
