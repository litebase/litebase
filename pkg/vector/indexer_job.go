package vector

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/sqlite3"
)

const (
	// IndexerBatchSize is the number of vectors to process in one batch
	IndexerBatchSize = 10000
)

// VectorIndexer processes pending vectors and assigns them to clusters
type VectorIndexer struct {
	DB             *database.DatabaseConnection
	TableName      string
	Dimensions     int
	DistanceMetric int
	MaxClusterSize int
	MinClusterSize int
	clusterer      *SPFreshClusterer
}

// NewVectorIndexer creates a new vector indexer
func NewVectorIndexer(db *database.DatabaseConnection, tableName string, dimensions, distanceMetric, maxClusterSize, minClusterSize int) (*VectorIndexer, error) {
	clusterer, err := NewSPFreshClusterer(db, tableName, dimensions, distanceMetric, maxClusterSize, minClusterSize)

	if err != nil {
		return nil, err
	}

	return &VectorIndexer{
		DB:             db,
		TableName:      tableName,
		Dimensions:     dimensions,
		DistanceMetric: distanceMetric,
		MaxClusterSize: maxClusterSize,
		MinClusterSize: minClusterSize,
		clusterer:      clusterer,
	}, nil
}

// ProcessBatch processes a batch of pending vectors using a transaction and batched operations
func (vi *VectorIndexer) ProcessBatch(ctx context.Context, batchSize int) (int, error) {
	// Check if context is already cancelled before starting work
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	var processed int
	var modifiedClusters []int64

	err := vi.DB.Transaction(false, func(db *database.DatabaseConnection) error {
		// Check context again inside transaction
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Get pending vectors - use rowid instead of id column since id may be NULL/0
		res, err := db.Exec(
			fmt.Sprintf(`SELECT rowid, vector_blob, operation FROM %s_pending ORDER BY created_at ASC LIMIT ?`, vi.TableName),
			[]sqlite3.StatementParameter{
				{Type: "INTEGER", Value: int64(batchSize)},
			},
		)

		if err != nil {
			return fmt.Errorf("failed to query pending vectors: %w", err)
		}

		var idsToDelete []int64

		// Collect batched operations
		var inserts []insertOp
		clusterSizeDeltas := make(map[int64]int) // cluster_id -> delta
		centroidUpdates := make(map[int64][]struct {
			vector    []float32
			operation string
		})

		now := time.Now().UTC().Unix()

		for _, row := range res.Rows {
			// Check context during loop processing
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if len(row) < 3 {
				continue
			}

			id := row[0].Int64()
			vectorBlob := row[1].Blob()
			operation := string(row[2].Text())

			// Process based on operation type
			switch operation {
			case "INSERT":
				// Parse vector
				vb, err := ParseVectorBlob(vectorBlob)

				if err != nil {
					slog.Error("Failed to parse vector blob", "id", id, "error", err)
					continue
				}

				vector := vb.GetFloat32Slice()

				if len(vector) != vi.Dimensions {
					slog.Error("Vector dimension mismatch", "id", id, "expected", vi.Dimensions, "got", len(vector))
					continue
				}

				// Check context before expensive clustering operation
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				// Assign to cluster
				clusterID, err := vi.clusterer.AssignToCluster(vector)

				if err != nil {
					slog.Error("Failed to assign to cluster", "id", id, "error", err)
					continue
				}

				// Get cluster version
				clusterRes, err := db.Exec(
					fmt.Sprintf(`SELECT version FROM %s_clusters WHERE cluster_id = ?`, vi.TableName),
					[]sqlite3.StatementParameter{
						{Type: sqlite3.ParameterTypeInteger, Value: int64(clusterID)},
					},
				)

				if err != nil || len(clusterRes.Rows) == 0 {
					slog.Error("Cluster not found", "cluster_id", clusterID, "error", err)
					continue
				}

				clusterVersion := clusterRes.Rows[0][0].Int64()

				// Collect insert operation
				inserts = append(inserts, insertOp{
					id:             id,
					clusterID:      clusterID,
					clusterVersion: clusterVersion,
					vectorBlob:     vectorBlob,
					indexedAt:      now,
				})

				// Track cluster size delta
				clusterSizeDeltas[clusterID]++

				// Collect centroid update
				centroidUpdates[clusterID] = append(centroidUpdates[clusterID], struct {
					vector    []float32
					operation string
				}{vector, "INSERT"})

			case "DELETE":
				// Get vector data before deleting
				delRes, err := db.Exec(
					fmt.Sprintf(`SELECT cluster_id, vector_blob FROM %s_indexed WHERE id = ?`, vi.TableName),
					[]sqlite3.StatementParameter{
						{Type: sqlite3.ParameterTypeInteger, Value: id},
					},
				)

				if err != nil || len(delRes.Rows) == 0 {
					// Vector not in indexed table, nothing to delete
					continue
				}

				clusterID := delRes.Rows[0][0].Int64()
				vectorBlob := delRes.Rows[0][1].Blob()

				// Delete from indexed table immediately (can't batch deletes easily)
				_, err = db.Exec(
					fmt.Sprintf(`DELETE FROM %s_indexed WHERE id = ?`, vi.TableName),
					[]sqlite3.StatementParameter{
						{Type: sqlite3.ParameterTypeInteger, Value: id},
					},
				)

				if err != nil {
					slog.Error("Failed to delete from indexed", "id", id, "error", err)
					continue
				}

				// Track cluster size delta
				clusterSizeDeltas[clusterID]--

				// Collect centroid update
				vb, _ := ParseVectorBlob(vectorBlob)

				if vb != nil {
					vector := vb.GetFloat32Slice()
					centroidUpdates[clusterID] = append(centroidUpdates[clusterID], struct {
						vector    []float32
						operation string
					}{vector, "DELETE"})
				}
			}

			idsToDelete = append(idsToDelete, id)
			processed++
		}

		// Batch insert into indexed table
		if len(inserts) > 0 {
			// Check context before database operation
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if err := vi.batchInsertIndexed(ctx, inserts); err != nil {
				return fmt.Errorf("failed to batch insert: %w", err)
			}
		}

		// Batch update cluster sizes
		if len(clusterSizeDeltas) > 0 {
			// Check context before database operation
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if err := vi.batchUpdateClusterSizes(ctx, clusterSizeDeltas); err != nil {
				return fmt.Errorf("failed to batch update cluster sizes: %w", err)
			}
		}

		// Update centroids
		for clusterID, updates := range centroidUpdates {
			// Check context before processing cluster updates
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			for _, update := range updates {
				if err := vi.clusterer.UpdateCentroid(clusterID, update.vector, update.operation); err != nil {
					slog.Error("Failed to update centroid", "cluster_id", clusterID, "error", err)
				}
			}
		}

		// Remove processed vectors from pending
		if len(idsToDelete) > 0 {
			if err := vi.deletePendingVectors(idsToDelete); err != nil {
				return fmt.Errorf("failed to delete pending vectors: %w", err)
			}
		}

		// Update pending count in metadata
		if err := vi.updatePendingCount(-processed); err != nil {
			slog.Error("Failed to update pending count", "error", err)
		}

		processed = len(idsToDelete)

		// Collect modified clusters for rebalancing outside transaction
		if len(clusterSizeDeltas) > 0 {
			modifiedClusters = make([]int64, 0, len(clusterSizeDeltas))

			for clusterID := range clusterSizeDeltas {
				modifiedClusters = append(modifiedClusters, clusterID)
			}
		}

		return nil
	})

	if err != nil {
		return 0, err
	}

	// Rebalance clusters OUTSIDE transaction to avoid blocking batch processing
	// This allows splits to happen asynchronously without affecting indexing throughput
	if processed > 0 && len(modifiedClusters) > 0 {
		if err := vi.clusterer.CheckAndRebalanceClusters(modifiedClusters); err != nil {
			slog.Error("Failed to rebalance clusters", "error", err)
			// Don't fail the batch if rebalancing fails - it can be retried later
		}
	}

	return processed, nil
}

// insertOp represents a batch insert operation
type insertOp struct {
	id             int64
	clusterID      int64
	clusterVersion int64
	vectorBlob     []byte
	indexedAt      int64
}

// batchInsertIndexed inserts multiple vectors into the indexed table in one operation
// Splits large batches into chunks to avoid SQLite variable limit (999 or 32766)
func (vi *VectorIndexer) batchInsertIndexed(ctx context.Context, inserts []insertOp) error {
	if len(inserts) == 0 {
		return nil
	}

	// SQLite has a limit on bind parameters (default 999, can be up to 32766)
	// With 5 params per row, we can safely do ~1500 rows per statement
	const maxRowsPerInsert = 1500

	// Process in chunks
	for i := 0; i < len(inserts); i += maxRowsPerInsert {
		// Check context before each chunk
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		end := i + maxRowsPerInsert

		if end > len(inserts) {
			end = len(inserts)
		}

		chunk := inserts[i:end]

		// Build multi-value INSERT statement for this chunk
		valuesParts := make([]string, len(chunk))
		params := make([]sqlite3.StatementParameter, 0, len(chunk)*5)

		for j, ins := range chunk {
			valuesParts[j] = "(?, ?, ?, ?, ?)"
			params = append(params,
				sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: ins.id},
				sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: ins.clusterID},
				sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: ins.clusterVersion},
				sqlite3.StatementParameter{Type: sqlite3.ParameterTypeBlob, Value: ins.vectorBlob},
				sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: ins.indexedAt},
			)
		}

		query := fmt.Sprintf(
			`INSERT OR REPLACE INTO %s_indexed (id, cluster_id, cluster_version, vector_blob, indexed_at) VALUES %s`,
			vi.TableName,
			strings.Join(valuesParts, ", "),
		)

		_, err := vi.DB.Exec(query, params)

		if err != nil {
			return fmt.Errorf("failed to insert chunk %d-%d: %w", i, end, err)
		}
	}

	return nil
}

// batchUpdateClusterSizes updates cluster sizes for multiple clusters in one operation
func (vi *VectorIndexer) batchUpdateClusterSizes(ctx context.Context, deltas map[int64]int) error {
	if len(deltas) == 0 {
		return nil
	}

	// Check context before database operation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Use a CASE statement to update multiple clusters efficiently
	var whenClauses []string
	var clusterIDs []string
	params := make([]sqlite3.StatementParameter, 0, len(deltas)*2)

	for clusterID, delta := range deltas {
		whenClauses = append(whenClauses, "WHEN cluster_id = ? THEN cluster_size + ?")
		clusterIDs = append(clusterIDs, "?")
		params = append(params,
			sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: clusterID},
			sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: int64(delta)},
		)
	}

	// Add cluster IDs for the WHERE clause
	for clusterID := range deltas {
		params = append(params,
			sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: clusterID},
		)
	}

	query := fmt.Sprintf(
		`UPDATE %s_clusters SET cluster_size = CASE %s END WHERE cluster_id IN (%s)`,
		vi.TableName,
		strings.Join(whenClauses, " "),
		strings.Join(clusterIDs, ", "),
	)

	_, err := vi.DB.Exec(query, params)

	return err
}

// deletePendingVectors removes processed vectors from the pending table
// Splits large batches into chunks to avoid SQLite's parameter limit (32766)
func (vi *VectorIndexer) deletePendingVectors(ids []int64) error {
	if len(ids) == 0 {
		return nil
	}

	// SQLite has a limit of 32766 bind parameters
	const maxParamsPerDelete = 32766

	// Process in chunks
	for i := 0; i < len(ids); i += maxParamsPerDelete {
		end := i + maxParamsPerDelete

		if end > len(ids) {
			end = len(ids)
		}

		chunk := ids[i:end]

		// Build placeholders for IN clause
		placeholders := make([]string, len(chunk))
		params := make([]sqlite3.StatementParameter, len(chunk))

		for j, id := range chunk {
			placeholders[j] = "?"
			params[j] = sqlite3.StatementParameter{
				Type:  sqlite3.ParameterTypeInteger,
				Value: int64(id),
			}
		}

		query := fmt.Sprintf(`DELETE FROM %s_pending WHERE id IN (%s)`, vi.TableName, strings.Join(placeholders, ","))

		_, err := vi.DB.Exec(query, params)

		if err != nil {
			return fmt.Errorf("failed to delete chunk %d-%d: %w", i, end, err)
		}
	}

	return nil
}

// updatePendingCount updates the pending_count in metadata
func (vi *VectorIndexer) updatePendingCount(delta int) error {
	// Get current count
	res, err := vi.DB.Exec(
		fmt.Sprintf(`SELECT value FROM %s_metadata WHERE key = 'pending_count'`, vi.TableName),
		nil,
	)

	var currentCount int

	if err == nil && len(res.Rows) > 0 {
		if _, err := fmt.Sscanf(string(res.Rows[0][0].Text()), "%d", &currentCount); err != nil {
			slog.Error("Failed to parse current pending_count", "error", err)
		}
	}

	newCount := max(currentCount+delta, 0)

	_, err = vi.DB.Exec(
		fmt.Sprintf(`INSERT OR REPLACE INTO %s_metadata (key, value) VALUES ('pending_count', ?)`, vi.TableName),
		[]sqlite3.StatementParameter{
			{Type: "TEXT", Value: []byte(fmt.Sprintf("%d", newCount))},
		},
	)

	return err
}
