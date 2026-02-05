package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/litebase/litebase/pkg/vector"
)

const (
	VectorIndexerBatchSize = 100000
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

	dbConn := conn.GetConnection()

	// Get vector columns configuration from metadata
	vectorColumns, err := vector.GetVectorColumns(dbConn, tableName)

	if err != nil {
		return fmt.Errorf("failed to get vector columns: %w", err)
	}

	// Get index configuration from metadata key-value store (table-level defaults)
	res, err := dbConn.Exec(
		fmt.Sprintf(`SELECT key, value FROM %s_metadata WHERE key IN ('max_cluster_size', 'min_cluster_size')`, tableName),
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

	var maxClusterSize, minClusterSize int

	fmt.Sscanf(metadata["max_cluster_size"], "%d", &maxClusterSize)
	fmt.Sscanf(metadata["min_cluster_size"], "%d", &minClusterSize)

	// Create indexer
	indexer, err := vector.NewVectorIndexer(
		dbConn,
		tableName,
		vectorColumns,
		maxClusterSize,
		minClusterSize,
	)

	if err != nil {
		return fmt.Errorf("failed to create vector indexer: %w", err)
	}
	// Process batches continuously until all pending vectors are done
	// This eliminates the pause between job dispatches
	totalProcessed := 0

	// Ensure connection is always released
	defer app.DatabaseManager.ConnectionManager().Release(conn)

	for {
		// Check context before processing each batch
		select {
		case <-ctx.Done():
			slog.Debug("VectorIndexer job cancelled",
				"db_id", dbID,
				"branch_id", branchID,
				"table", tableName,
				"processed", totalProcessed)
			return ctx.Err()
		default:
		}

		processed, err := indexer.ProcessBatch(ctx, VectorIndexerBatchSize)

		if err != nil {
			// Check if this is a SQLite interrupt error (expected during shutdown)
			// SQLite returns errors with string messages, so check for "interrupt"
			if strings.Contains(strings.ToLower(err.Error()), "interrupt") || errors.Is(err, context.Canceled) {
				slog.Debug("VectorIndexer job interrupted during shutdown",
					"db_id", dbID,
					"branch_id", branchID,
					"table", tableName,
					"processed", totalProcessed)
				return nil // Treat interrupt as graceful shutdown
			}

			return fmt.Errorf("failed to process batch: %w", err)
		}

		totalProcessed += processed

		slog.Debug("Processed vector indexing batch",
			"db_id", dbID,
			"branch_id", branchID,
			"table", tableName,
			"batch_processed", processed,
			"total_processed", totalProcessed)

		// If we processed less than a full batch, we're done
		if processed < VectorIndexerBatchSize {
			slog.Info("VectorIndexer job completed",
				"db_id", dbID,
				"branch_id", branchID,
				"table", tableName,
				"total_processed", totalProcessed)
			return nil
		}
	}
}
