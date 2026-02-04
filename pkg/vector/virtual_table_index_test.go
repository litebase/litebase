package vector_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/sqlite3"
)

func TestVectorIndexCreate(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testDb := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(testDb.DatabaseID, testDb.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		// Create a vector index table
		_, err = conn.GetConnection().Exec(`
			CREATE VIRTUAL TABLE test_vectors USING vector_index(
				vector BLOB,
				dimensions=128,
				distance_metric='cosine'
			)
		`, nil)

		if err != nil {
			t.Fatalf("Failed to create vector index: %v", err)
		}

		// Verify shadow tables were created
		shadowTables := []string{
			"test_vectors_pending",
			"test_vectors_clusters",
			"test_vectors_indexed",
			"test_vectors_stats",
			"test_vectors_metadata",
		}

		for _, table := range shadowTables {
			res, err := conn.GetConnection().Exec("SELECT name FROM sqlite_master WHERE type='table' AND name=?", []sqlite3.StatementParameter{
				{
					Type:  "TEXT",
					Value: []byte(table),
				},
			})

			if err != nil || len(res.Rows) == 0 {
				t.Errorf("Shadow table %s not found: %v", table, err)
			}
		}

		// Verify metadata was stored correctly
		res, err := conn.GetConnection().Exec("SELECT value FROM test_vectors_metadata WHERE key='dimensions'", nil)

		if err != nil {
			t.Fatalf("Failed to read dimensions metadata: %v", err)
		}

		if len(res.Rows) == 0 || string(res.Rows[0][0].Text()) != "128" {
			t.Errorf("Expected dimensions=128, got %s", string(res.Rows[0][0].Text()))
		}

		res, err = conn.GetConnection().Exec("SELECT value FROM test_vectors_metadata WHERE key='distance_metric'", nil)

		if err != nil {
			t.Fatalf("Failed to read distance_metric metadata: %v", err)
		}

		// DISTANCE_METRIC_COSINE = 1 (from constants.go)
		if len(res.Rows) == 0 || string(res.Rows[0][0].Text()) != "1" {
			t.Errorf("Expected distance_metric=1 (cosine), got %s", string(res.Rows[0][0].Text()))
		}

		res, err = conn.GetConnection().Exec("SELECT value FROM test_vectors_metadata WHERE key='max_cluster_size'", nil)

		if err != nil {
			t.Fatalf("Failed to read max_cluster_size metadata: %v", err)
		}

		if len(res.Rows) == 0 || string(res.Rows[0][0].Text()) != "5000" {
			t.Errorf("Expected max_cluster_size=5000, got %s", string(res.Rows[0][0].Text()))
		}

		res, err = conn.GetConnection().Exec("SELECT value FROM test_vectors_metadata WHERE key='min_cluster_size'", nil)

		if err != nil {
			t.Fatalf("Failed to read min_cluster_size metadata: %v", err)
		}

		if len(res.Rows) == 0 || string(res.Rows[0][0].Text()) != "200" {
			t.Errorf("Expected min_cluster_size=200, got %s", string(res.Rows[0][0].Text()))
		}
	})
}
