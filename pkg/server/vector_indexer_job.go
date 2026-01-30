package server

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/litebase/litebase/pkg/vector"
)

const (
	VectorIndexerBatchSize = 5000
)

// VectorIndexerJob processes pending vectors for a specific index
func VectorIndexerJob(ctx context.Context, app *App, data map[string]interface{}) error {
	// Only run on primary node
	if !app.Cluster.Node().IsPrimary() {
		return nil
	}

	// Extract job parameters - support both db_name (old) and db_id (new)
	var dbID string

	if id, ok := data["db_id"].(string); ok {
		dbID = id
	} else if name, ok := data["db_name"].(string); ok {
		// Legacy support - look up by name
		database, err := app.DatabaseManager.GetByName(name)

		if err != nil {
			return fmt.Errorf("failed to get database by name: %w", err)
		}

		dbID = database.DatabaseID
	} else {
		return fmt.Errorf("missing db_id or db_name")
	}

	// Extract branch ID - support both branch_name (old) and branch_id (new)
	var branchID string

	if id, ok := data["branch_id"].(string); ok {
		branchID = id
	} else if name, ok := data["branch_name"].(string); ok {
		// Legacy support - look up by name
		database, err := app.DatabaseManager.Get(dbID)

		if err != nil {
			return fmt.Errorf("failed to get database: %w", err)
		}

		branch, err := database.Branch(name)

		if err != nil {
			return fmt.Errorf("failed to get branch: %w", err)
		}

		branchID = branch.DatabaseBranchID
	} else {
		return fmt.Errorf("missing branch_id or branch_name")
	}

	tableName, ok := data["table_name"].(string)

	if !ok {
		return fmt.Errorf("missing or invalid table_name")
	}

	// Mark as processing started
	defer app.VectorIndexMgr.MarkProcessed(dbID, branchID, tableName)

	// Get database connection using ConnectionManager
	conn, err := app.DatabaseManager.ConnectionManager().Get(dbID, branchID)

	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}

	defer app.DatabaseManager.ConnectionManager().Release(conn)

	dbConn := conn.GetConnection()

	// Get index configuration from metadata key-value store
	res, err := dbConn.Exec(
		fmt.Sprintf(`SELECT key, value FROM %s_metadata WHERE key IN ('dimensions', 'distance_metric', 'max_cluster_size', 'min_cluster_size')`, tableName),
		nil,
	)

	if err != nil || len(res.Rows) == 0 {
		return fmt.Errorf("failed to get index configuration: %w", err)
	}

	// Parse metadata into a map
	metadata := make(map[string]string)

	for _, row := range res.Rows {
		key := string(row[0].Text())
		value := string(row[1].Text())
		metadata[key] = value
	}

	var dimensions, distanceMetric, maxClusterSize, minClusterSize int

	fmt.Sscanf(metadata["dimensions"], "%d", &dimensions)
	fmt.Sscanf(metadata["distance_metric"], "%d", &distanceMetric)
	fmt.Sscanf(metadata["max_cluster_size"], "%d", &maxClusterSize)
	fmt.Sscanf(metadata["min_cluster_size"], "%d", &minClusterSize)

	// Create indexer
	indexer, err := vector.NewVectorIndexer(
		dbConn,
		tableName,
		dimensions,
		distanceMetric,
		maxClusterSize,
		minClusterSize,
	)

	if err != nil {
		return fmt.Errorf("failed to create vector indexer: %w", err)
	}

	// Process batch
	processed, err := indexer.ProcessBatch(ctx, VectorIndexerBatchSize)

	if err != nil {
		return fmt.Errorf("failed to process batch: %w", err)
	}

	slog.Info("Processed vector indexing batch",
		"db_id", dbID,
		"branch_id", branchID,
		"table", tableName,
		"processed", processed)

	// Check if there are more pending vectors to process
	// If we processed a full batch, there might be more work to do
	if processed >= VectorIndexerBatchSize {
		// Query for remaining pending count
		res, err := dbConn.Exec(
			fmt.Sprintf(`SELECT COUNT(*) FROM %s_pending`, tableName),
			nil,
		)

		if err == nil && len(res.Rows) > 0 && res.Rows[0][0].Int64() > 0 {
			// There are more pending vectors, notify the manager
			slog.Debug("More vectors pending after batch",
				"table", tableName,
				"remaining", res.Rows[0][0].Int64())

			app.VectorIndexMgr.MarkPending(dbID, branchID, tableName)
		}
	}

	return nil
}
