package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/litebase/litebase/pkg/database"
)

const (
	VectorIndexerBatchSize = 50000
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
	vectorColumns, err := database.GetVectorColumns(dbConn, tableName)

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
	indexer, err := database.NewVectorIndexer(
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
			// Robust completion check: sum cluster-0 counts across all vector columns.
			// Retry a few times with short backoff to avoid transient races.
			var totalCluster0 int64
			var lastErr error

			for attempt := 0; attempt < 3; attempt++ {
				totalCluster0 = 0
				lastErr = nil

				for _, col := range vectorColumns {
					// Build table name for this column's cluster_vector_map
					table := tableName + "_" + col.Name + "_cluster_vector_map"
					q := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE cluster_id = 0", table)

					res, err := dbConn.Exec(q, nil)

					if err != nil {
						lastErr = err
						break
					}

					if len(res.Rows) == 0 {
						continue
					}

					cnt := res.Rows[0][0].Int64()
					totalCluster0 += cnt
					slog.Debug("Cluster0 count", "table", table, "count", cnt)
				}

				if lastErr != nil {
					// transient error; small backoff then retry
					time.Sleep(50 * time.Millisecond)
					continue
				}

				// If any vectors remain, continue processing
				if totalCluster0 > 0 {
					slog.Debug("Cluster 0 still has vectors, continuing processing",
						"db_id", dbID,
						"branch_id", branchID,
						"table", tableName,
						"cluster0_total", totalCluster0)
					break
				}

				// No vectors found across columns; safe to finish
				if totalCluster0 == 0 {
					slog.Info("VectorIndexer job completed",
						"db_id", dbID,
						"branch_id", branchID,
						"table", tableName,
						"total_processed", totalProcessed)
					return nil
				}
			}

			if lastErr != nil {
				// If we couldn't reliably count, log and continue (avoid false completion)
				slog.Warn("Failed to verify cluster0 counts; will continue processing",
					"error", lastErr)
			}
		}
	}
}
