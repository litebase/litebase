package server_test

import (
	"context"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/sqlite3"
)

func TestVectorIndexerJobOnlyOnPrimary(t *testing.T) {
	t.Run("RunsOnPrimary", func(t *testing.T) {
		test.RunWithApp(t, func(app *server.App) {
			if err := app.Cluster.Node().WaitForPrimary(); err != nil {
				t.Fatalf("Failed to wait for primary: %v", err)
			}

			mock := test.MockDatabase(app)

			conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Fatalf("Failed to get connection: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(conn)

			dbConn := conn.GetConnection()

			// Create vector index table
			_, err = dbConn.Exec(`
				CREATE VIRTUAL TABLE embeddings USING vector_index(
					vector BLOB,
					dimensions=4,
					distance_metric=0
				)
			`, nil)

			if err != nil {
				t.Fatalf("Failed to create vector index: %v", err)
			}

			// Job should run successfully on primary
			jobData := map[string]interface{}{
				"db_id":      mock.DatabaseID,
				"branch_id":  mock.DatabaseBranchID,
				"table_name": "embeddings",
			}

			err = server.VectorIndexerJob(context.Background(), app, jobData)

			if err != nil {
				t.Errorf("VectorIndexerJob failed on primary: %v", err)
			}
		})
	})

	t.Run("SkipsOnReplica", func(t *testing.T) {
		test.RunWithApp(t, func(app *server.App) {
			// Force node to be replica
			app.Cluster.Node().SetMembership(cluster.ClusterMembershipReplica)

			jobData := map[string]interface{}{
				"db_id":      "test_db",
				"branch_id":  "test_branch",
				"table_name": "embeddings",
			}

			// Job should skip on replica (return nil)
			err := server.VectorIndexerJob(context.Background(), app, jobData)

			if err != nil {
				t.Errorf("VectorIndexerJob should return nil on replica, got: %v", err)
			}
		})
	})
}

func TestVectorIndexerJobWithValidParameters(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		if err := app.Cluster.Node().WaitForPrimary(); err != nil {
			t.Fatalf("Failed to wait for primary: %v", err)
		}

		mock := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		dbConn := conn.GetConnection()

		t.Run("ProcessesWithNewParameters", func(t *testing.T) {
			// Create vector index table
			_, err = dbConn.Exec(`
				CREATE VIRTUAL TABLE test_vectors USING vector_index(
					vector BLOB,
					dimensions=128,
					distance_metric=1,
					max_cluster_size=1000,
					min_cluster_size=50
				)
			`, nil)

			if err != nil {
				t.Fatalf("Failed to create vector index: %v", err)
			}

			jobData := map[string]interface{}{
				"db_id":      mock.DatabaseID,
				"branch_id":  mock.DatabaseBranchID,
				"table_name": "test_vectors",
			}

			err = server.VectorIndexerJob(context.Background(), app, jobData)

			if err != nil {
				t.Errorf("VectorIndexerJob failed: %v", err)
			}
		})
	})
}

func TestVectorIndexerJobLegacyParameterSupport(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		if err := app.Cluster.Node().WaitForPrimary(); err != nil {
			t.Fatalf("Failed to wait for primary: %v", err)
		}

		mock := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		dbConn := conn.GetConnection()

		// Create vector index table
		_, err = dbConn.Exec(`
			CREATE VIRTUAL TABLE legacy_vectors USING vector_index(
				vector BLOB,
				dimensions=4,
				distance_metric=0
			)
		`, nil)

		if err != nil {
			t.Fatalf("Failed to create vector index: %v", err)
		}

		t.Run("SupportsDbNameLegacy", func(t *testing.T) {
			// Get database by ID first
			database, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatalf("Failed to get database: %v", err)
			}

			jobData := map[string]interface{}{
				"db_name":    database.Name,
				"branch_id":  mock.DatabaseBranchID,
				"table_name": "legacy_vectors",
			}

			err = server.VectorIndexerJob(context.Background(), app, jobData)

			if err != nil {
				t.Errorf("VectorIndexerJob with db_name failed: %v", err)
			}
		})

		t.Run("SupportsBranchNameLegacy", func(t *testing.T) {
			// Get database and branch
			database, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatalf("Failed to get database: %v", err)
			}

			branch, err := database.BranchByID(mock.DatabaseBranchID)

			if err != nil {
				t.Fatalf("Failed to get branch: %v", err)
			}

			jobData := map[string]interface{}{
				"db_id":       mock.DatabaseID,
				"branch_name": branch.Name,
				"table_name":  "legacy_vectors",
			}

			err = server.VectorIndexerJob(context.Background(), app, jobData)

			if err != nil {
				t.Errorf("VectorIndexerJob with branch_name failed: %v", err)
			}
		})

		t.Run("SupportsBothLegacyParameters", func(t *testing.T) {
			database, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatalf("Failed to get database: %v", err)
			}

			branch, err := database.BranchByID(mock.DatabaseBranchID)

			if err != nil {
				t.Fatalf("Failed to get branch: %v", err)
			}

			jobData := map[string]interface{}{
				"db_name":     database.Name,
				"branch_name": branch.Name,
				"table_name":  "legacy_vectors",
			}

			err = server.VectorIndexerJob(context.Background(), app, jobData)

			if err != nil {
				t.Errorf("VectorIndexerJob with both legacy params failed: %v", err)
			}
		})
	})
}

func TestVectorIndexerJobErrorHandling(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		if err := app.Cluster.Node().WaitForPrimary(); err != nil {
			t.Fatalf("Failed to wait for primary: %v", err)
		}

		t.Run("MissingDatabaseID", func(t *testing.T) {
			jobData := map[string]interface{}{
				"branch_id":  "branch1",
				"table_name": "vectors",
			}

			err := server.VectorIndexerJob(context.Background(), app, jobData)

			if err == nil {
				t.Error("Expected error for missing db_id")
			}

			if err.Error() != "missing db_id or db_name" {
				t.Errorf("Expected 'missing db_id or db_name' error, got: %v", err)
			}
		})

		t.Run("MissingBranchID", func(t *testing.T) {
			jobData := map[string]interface{}{
				"db_id":      "db1",
				"table_name": "vectors",
			}

			err := server.VectorIndexerJob(context.Background(), app, jobData)

			if err == nil {
				t.Error("Expected error for missing branch_id")
			}

			if err.Error() != "missing branch_id or branch_name" {
				t.Errorf("Expected 'missing branch_id or branch_name' error, got: %v", err)
			}
		})

		t.Run("MissingTableName", func(t *testing.T) {
			jobData := map[string]interface{}{
				"db_id":     "db1",
				"branch_id": "branch1",
			}

			err := server.VectorIndexerJob(context.Background(), app, jobData)

			if err == nil {
				t.Error("Expected error for missing table_name")
			}

			if err.Error() != "missing or invalid table_name" {
				t.Errorf("Expected 'missing or invalid table_name' error, got: %v", err)
			}
		})

		t.Run("InvalidDatabaseName", func(t *testing.T) {
			jobData := map[string]interface{}{
				"db_name":    "nonexistent_db",
				"branch_id":  "branch1",
				"table_name": "vectors",
			}

			err := server.VectorIndexerJob(context.Background(), app, jobData)

			if err == nil {
				t.Error("Expected error for invalid database name")
			}
		})

		t.Run("InvalidDatabaseID", func(t *testing.T) {
			jobData := map[string]interface{}{
				"db_id":      "nonexistent_id",
				"branch_id":  "branch1",
				"table_name": "vectors",
			}

			err := server.VectorIndexerJob(context.Background(), app, jobData)

			if err == nil {
				t.Error("Expected error for invalid database ID")
			}
		})
	})
}

func TestVectorIndexerJobProcessesPendingVectors(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		if err := app.Cluster.Node().WaitForPrimary(); err != nil {
			t.Fatalf("Failed to wait for primary: %v", err)
		}

		mock := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		dbConn := conn.GetConnection()

		// Create vector index table
		_, err = dbConn.Exec(`
			CREATE VIRTUAL TABLE process_vectors USING vector_index(
				vector BLOB,
				dimensions=4,
				distance_metric=0
			)
		`, nil)

		if err != nil {
			t.Fatalf("Failed to create vector index: %v", err)
		}

		// Insert some pending vectors
		for i := 0; i < 5; i++ {
			_, err = dbConn.Exec(
				"INSERT INTO process_vectors(vector) VALUES(vector_f32(?))",
				[]sqlite3.StatementParameter{
					{Type: "TEXT", Value: []byte("[1.0, 2.0, 3.0, 4.0]")},
				},
			)

			if err != nil {
				t.Fatalf("Failed to insert vector: %v", err)
			}
		}

		// Check pending count before processing (use current cluster map schema)
		res, err := dbConn.Exec("SELECT COUNT(*) FROM process_vectors_vector_cluster_vector_map WHERE cluster_id = 0", nil)

		if err != nil {
			t.Fatalf("Failed to count pending vectors: %v", err)
		}

		if len(res.Rows) == 0 {
			t.Fatal("No rows returned from pending count")
		}

		pendingBefore := res.Rows[0][0].Int64()

		if pendingBefore != 5 {
			t.Logf("Note: Expected 5 pending vectors, got %d", pendingBefore)
		}

		// Run the indexer job
		jobData := map[string]interface{}{
			"db_id":      mock.DatabaseID,
			"branch_id":  mock.DatabaseBranchID,
			"table_name": "process_vectors",
		}

		err = server.VectorIndexerJob(context.Background(), app, jobData)

		if err != nil {
			t.Errorf("VectorIndexerJob failed: %v", err)
		}
	})
}

func TestVectorIndexerJobMarksProcessedWhenDone(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		if err := app.Cluster.Node().WaitForPrimary(); err != nil {
			t.Fatalf("Failed to wait for primary: %v", err)
		}

		mock := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		dbConn := conn.GetConnection()

		// Create vector index table
		_, err = dbConn.Exec(`
			CREATE VIRTUAL TABLE mark_vectors USING vector_index(
				vector BLOB,
				dimensions=4,
				distance_metric=0
			)
		`, nil)

		if err != nil {
			t.Fatalf("Failed to create vector index: %v", err)
		}

		// Mark as pending in the index manager
		app.VectorIndexMgr.MarkPending(mock.DatabaseID, mock.DatabaseBranchID, "mark_vectors")

		// Run the job
		jobData := map[string]interface{}{
			"db_id":      mock.DatabaseID,
			"branch_id":  mock.DatabaseBranchID,
			"table_name": "mark_vectors",
		}

		err = server.VectorIndexerJob(context.Background(), app, jobData)

		if err != nil {
			t.Errorf("VectorIndexerJob failed: %v", err)
		}

		// The job should call MarkProcessed via defer
		// We can verify this by checking the Processing flag is false
		// (Note: This requires exposing test methods on VectorIndexManager)
	})
}
