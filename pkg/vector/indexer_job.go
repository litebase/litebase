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
	IndexerBatchSize = 5000
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
	// Begin transaction for better performance
	if err := vi.DB.Begin(); err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Get pending vectors - use rowid instead of id column since id may be NULL/0
	res, err := vi.DB.Exec(
		fmt.Sprintf(`SELECT rowid, vector_blob, operation FROM %s_pending ORDER BY created_at ASC LIMIT ?`, vi.TableName),
		[]sqlite3.StatementParameter{
			{Type: "INTEGER", Value: int64(batchSize)},
		},
	)

	if err != nil {
		return 0, fmt.Errorf("failed to query pending vectors: %w", err)
	}

	var processed int
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

			// Assign to cluster
			clusterID, err := vi.clusterer.AssignToCluster(vector)

			if err != nil {
				slog.Error("Failed to assign to cluster", "id", id, "error", err)
				continue
			}

			// Get cluster version
			clusterRes, err := vi.DB.Exec(
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
			delRes, err := vi.DB.Exec(
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
			_, err = vi.DB.Exec(
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
		if err := vi.batchInsertIndexed(inserts); err != nil {
			return 0, fmt.Errorf("failed to batch insert: %w", err)
		}
	}

	// Batch update cluster sizes
	if len(clusterSizeDeltas) > 0 {
		if err := vi.batchUpdateClusterSizes(clusterSizeDeltas); err != nil {
			return 0, fmt.Errorf("failed to batch update cluster sizes: %w", err)
		}
	}

	// Update centroids
	for clusterID, updates := range centroidUpdates {
		for _, update := range updates {
			if err := vi.clusterer.UpdateCentroid(clusterID, update.vector, update.operation); err != nil {
				slog.Error("Failed to update centroid", "cluster_id", clusterID, "error", err)
			}
		}
	}

	// Remove processed vectors from pending
	if len(idsToDelete) > 0 {
		if err := vi.deletePendingVectors(idsToDelete); err != nil {
			return 0, fmt.Errorf("failed to delete pending vectors: %w", err)
		}
	}

	// Update pending count in metadata
	if err := vi.updatePendingCount(-processed); err != nil {
		slog.Error("Failed to update pending count", "error", err)
	}

	// Commit transaction
	if err := vi.DB.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Check if clusters need rebalancing (outside transaction)
	if processed > 0 {
		if err := vi.clusterer.CheckAndRebalance(); err != nil {
			slog.Error("Failed to rebalance clusters", "error", err)
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
func (vi *VectorIndexer) batchInsertIndexed(inserts []insertOp) error {
	if len(inserts) == 0 {
		return nil
	}

	// Build multi-value INSERT statement
	valuesParts := make([]string, len(inserts))
	params := make([]sqlite3.StatementParameter, 0, len(inserts)*5)

	for i, ins := range inserts {
		valuesParts[i] = "(?, ?, ?, ?, ?)"
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

	return err
}

// batchUpdateClusterSizes updates cluster sizes for multiple clusters in one operation
func (vi *VectorIndexer) batchUpdateClusterSizes(deltas map[int64]int) error {
	if len(deltas) == 0 {
		return nil
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
func (vi *VectorIndexer) deletePendingVectors(ids []int64) error {
	// Build placeholders for IN clause
	placeholders := make([]string, len(ids))
	params := make([]sqlite3.StatementParameter, len(ids))

	for i, id := range ids {
		placeholders[i] = "?"
		params[i] = sqlite3.StatementParameter{
			Type:  sqlite3.ParameterTypeInteger,
			Value: int64(id),
		}
	}

	query := fmt.Sprintf(`DELETE FROM %s_pending WHERE id IN (%s)`, vi.TableName, strings.Join(placeholders, ","))

	_, err := vi.DB.Exec(query, params)

	return err
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
