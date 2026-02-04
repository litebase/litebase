package vector

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"

	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/sqlite3"
)

const (
	// IndexerBatchSize is the number of vectors to process in one batch
	IndexerBatchSize = 10000
)

// getVectorColumnName queries the metadata table to get the first vector column name
func getVectorColumnName(db *database.DatabaseConnection, tableName string) (string, error) {
	res, err := db.Exec(
		fmt.Sprintf("SELECT value FROM %s_metadata WHERE key = 'vector_column'", tableName),
		nil,
	)

	if err != nil {
		return "", fmt.Errorf("failed to query vector_column from metadata: %w", err)
	}

	if len(res.Rows) == 0 || len(res.Rows[0]) == 0 {
		// Fall back to "vector_blob" for backward compatibility with old indexes
		return "vector_blob", nil
	}

	columnName := string(res.Rows[0][0].Text())

	return columnName, nil
}

// VectorIndexer processes vectors in cluster 0 and reassigns them to proper clusters
type VectorIndexer struct {
	DB             *database.DatabaseConnection
	TableName      string
	Dimensions     int
	DistanceMetric int
	MaxClusterSize int
	MinClusterSize int
}

// NewVectorIndexer creates a new vector indexer
func NewVectorIndexer(db *database.DatabaseConnection, tableName string, dimensions, distanceMetric, maxClusterSize, minClusterSize int) (*VectorIndexer, error) {
	return &VectorIndexer{
		DB:             db,
		TableName:      tableName,
		Dimensions:     dimensions,
		DistanceMetric: distanceMetric,
		MaxClusterSize: maxClusterSize,
		MinClusterSize: minClusterSize,
	}, nil
}

// ProcessBatch processes vectors assigned to cluster 0 and reassigns them to proper clusters
// For hierarchical IVF with "cluster 0" fast indexing:
// - Reads vectors from cluster 0 (temporary assignment)
// - Reassigns to proper leaf clusters using hierarchical traversal
// - Updates _cluster_vector_map mappings
// - Updates cluster centroids and sizes in _cluster_tree
func (vi *VectorIndexer) ProcessBatch(ctx context.Context, batchSize int) (int, error) {
	// Check if context is already cancelled before starting work
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	var processed int

	err := vi.DB.Transaction(false, func(db *database.DatabaseConnection) error {
		// Check context again inside transaction
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Get the vector column name from metadata
		vectorColumn, err := getVectorColumnName(db, vi.TableName)

		if err != nil {
			return fmt.Errorf("failed to get vector column name: %w", err)
		}

		// Get vectors currently assigned to cluster 0 (need reassignment)
		res, err := db.Exec(
			fmt.Sprintf(`
				SELECT v.id, v.%s
				FROM %s_vectors v 
				INNER JOIN %s_cluster_vector_map m ON v.id = m.vector_id 
				WHERE m.cluster_id = 0 
				ORDER BY v.created_at ASC 
				LIMIT ?`,
				vectorColumn, vi.TableName, vi.TableName),
			[]sqlite3.StatementParameter{
				{Type: "INTEGER", Value: int64(batchSize)},
			},
		)

		if err != nil {
			return fmt.Errorf("failed to query vectors in cluster 0: %w", err)
		}

		slog.Debug("Queried cluster 0 vectors",
			"table", vi.TableName,
			"batch_size", batchSize,
			"found", len(res.Rows))

		if len(res.Rows) == 0 {
			// No vectors to reassign
			return nil
		}

		// Track cluster updates
		clusterSizeDeltas := make(map[int64]int)       // cluster_id -> size change
		clusterVectorSums := make(map[int64][]float32) // cluster_id -> sum of vectors for centroid update
		vectorAssignments := make(map[int64]int64)     // vector_id -> new cluster_id
		vectorDistances := make(map[int64]float64)     // vector_id -> distance to cluster centroid

		now := time.Now().UTC().Unix()

		for _, row := range res.Rows {
			// Check context during loop processing
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if len(row) < 2 {
				continue
			}

			vectorID := row[0].Int64()
			vectorBlob := row[1].Blob()

			// Parse vector
			vb, err := ParseVectorBlob(vectorBlob)

			if err != nil {
				slog.Error("Failed to parse vector blob", "id", vectorID, "error", err)
				continue
			}

			vector := vb.GetFloat32Slice()

			if len(vector) != vi.Dimensions {
				slog.Error("Vector dimension mismatch", "id", vectorID, "expected", vi.Dimensions, "got", len(vector))
				continue
			}

			// Find best cluster using hierarchical traversal
			clusterID, distance, err := vi.findBestCluster(db, vector)

			if err != nil {
				slog.Error("Failed to find best cluster", "id", vectorID, "error", err)
				continue
			}

			// Record assignment and distance
			vectorAssignments[vectorID] = clusterID
			vectorDistances[vectorID] = distance
			clusterSizeDeltas[clusterID]++

			// Accumulate vector for centroid update
			if _, exists := clusterVectorSums[clusterID]; !exists {
				clusterVectorSums[clusterID] = make([]float32, len(vector))
			}

			for i, v := range vector {
				clusterVectorSums[clusterID][i] += v
			}
		}

		// Update cluster mappings in batch
		if len(vectorAssignments) > 0 {
			// SQLite has a limit on the number of parameters (default 999, max 32766)
			// With 4 params per row (vector_id, cluster_id, distance, indexed_at),
			// we can safely do 8000 rows per chunk (32000 parameters)
			const maxRowsPerBatch = 8000

			// Convert map to slice for chunking
			assignments := make([]struct {
				vectorID  int64
				clusterID int64
				distance  float64
			}, 0, len(vectorAssignments))

			for vectorID, clusterID := range vectorAssignments {
				assignments = append(assignments, struct {
					vectorID  int64
					clusterID int64
					distance  float64
				}{vectorID, clusterID, vectorDistances[vectorID]})
			}

			// Process in chunks
			for i := 0; i < len(assignments); i += maxRowsPerBatch {
				end := i + maxRowsPerBatch

				if end > len(assignments) {
					end = len(assignments)
				}

				chunk := assignments[i:end]

				// Build bulk UPDATE statement for this chunk
				valuesParts := make([]string, 0, len(chunk))
				params := make([]sqlite3.StatementParameter, 0, len(chunk)*4)

				for _, assignment := range chunk {
					valuesParts = append(valuesParts, "(?, ?, ?, ?)")
					params = append(params,
						sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: assignment.vectorID},
						sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: assignment.clusterID},
						sqlite3.StatementParameter{Type: sqlite3.ParameterTypeFloat, Value: assignment.distance},
						sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: now},
					)
				}

				// Use INSERT OR REPLACE to update cluster assignments
				query := fmt.Sprintf(
					`INSERT OR REPLACE INTO %s_cluster_vector_map (vector_id, cluster_id, distance, indexed_at) VALUES %s`,
					vi.TableName,
					strings.Join(valuesParts, ", "),
				)

				if _, err := db.Exec(query, params); err != nil {
					return fmt.Errorf("failed to update cluster mappings chunk %d-%d: %w", i, end, err)
				}
			}
		}

		// Update cluster sizes
		if len(clusterSizeDeltas) > 0 {
			// Decrement cluster 0 size
			cluster0Delta := -len(vectorAssignments)

			if err := vi.updateClusterSize(db, 0, cluster0Delta); err != nil {
				slog.Error("Failed to update cluster 0 size", "error", err)
			}

			// Increment target cluster sizes
			for clusterID, delta := range clusterSizeDeltas {
				if err := vi.updateClusterSize(db, clusterID, delta); err != nil {
					slog.Error("Failed to update cluster size", "cluster", clusterID, "error", err)
				}
			}
		}

		// Update cluster centroids
		for clusterID, vectorSum := range clusterVectorSums {
			count := clusterSizeDeltas[clusterID]

			if count == 0 {
				continue
			}

			// Get current centroid and size
			currentCentroid, currentSize, err := vi.getClusterCentroid(db, clusterID)

			if err != nil {
				slog.Error("Failed to get current centroid", "cluster", clusterID, "error", err)
				continue
			}

			// Calculate new centroid: (old_centroid * old_size + vector_sum) / new_size
			newSize := currentSize + count
			newCentroid := make([]float32, len(vectorSum))

			for i := range newCentroid {
				oldContribution := currentCentroid[i] * float32(currentSize)
				newContribution := vectorSum[i]
				newCentroid[i] = (oldContribution + newContribution) / float32(newSize)
			}

			// Serialize new centroid
			centroidBlob, err := EncodeFloat32(newCentroid)

			if err != nil {
				slog.Error("Failed to serialize centroid", "cluster", clusterID, "error", err)
				continue
			}

			// Update centroid in _cluster_tree
			_, err = db.Exec(
				fmt.Sprintf(`UPDATE %s_cluster_tree SET centroid_blob = ? WHERE cluster_id = ?`, vi.TableName),
				[]sqlite3.StatementParameter{
					{Type: sqlite3.ParameterTypeBlob, Value: centroidBlob},
					{Type: sqlite3.ParameterTypeInteger, Value: clusterID},
				},
			)

			if err != nil {
				slog.Error("Failed to update centroid", "cluster", clusterID, "error", err)
			}
		}

		processed = len(vectorAssignments)

		return nil
	})

	if err != nil {
		return 0, err
	}

	// After successful batch processing, check if any clusters need splitting
	// This happens OUTSIDE the transaction to avoid long-running locks
	if processed > 0 && vi.MaxClusterSize > 0 {
		select {
		case <-ctx.Done():
			return processed, ctx.Err()
		default:
		}

		slog.Info("Checking for oversized clusters after batch",
			"table", vi.TableName,
			"processed", processed,
			"max_cluster_size", vi.MaxClusterSize)

		if err := vi.splitOversizedClusters(ctx); err != nil {
			slog.Error("Failed to split oversized clusters", "error", err)
			// Don't fail the batch - splitting can be retried later
		}
	} else {
		slog.Debug("Skipping cluster splitting",
			"table", vi.TableName,
			"processed", processed,
			"max_cluster_size", vi.MaxClusterSize)
	}

	return processed, nil
}

// findBestCluster finds the best leaf cluster for a vector using hierarchical traversal
// Returns the cluster ID and the distance from the vector to the cluster's centroid
func (vi *VectorIndexer) findBestCluster(db *database.DatabaseConnection, vector []float32) (int64, float64, error) {
	// Start from root (cluster_id = 1)
	currentClusterID := int64(1)
	finalDistance := float64(0)

	for {
		// Check if current cluster is a leaf
		res, err := db.Exec(
			fmt.Sprintf(`SELECT is_leaf, centroid_blob FROM %s_cluster_tree WHERE cluster_id = ?`, vi.TableName),
			[]sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeInteger, Value: currentClusterID},
			},
		)

		if err != nil || len(res.Rows) == 0 {
			// Cluster doesn't exist - need to create initial structure
			clusterID, err := vi.getOrCreateRootCluster(db)
			return clusterID, 0, err
		}

		isLeaf := res.Rows[0][0].Int64() == 1
		centroidBlob := res.Rows[0][1].Blob()

		// Calculate distance to current cluster's centroid
		centroid, err := ParseVectorBlob(centroidBlob)

		if err == nil {
			finalDistance = vi.calculateDistance(vector, centroid.GetFloat32Slice())
		}

		if isLeaf {
			// Found a leaf cluster, return it with distance
			return currentClusterID, finalDistance, nil
		}

		// Not a leaf - find best child cluster
		childRes, err := db.Exec(
			fmt.Sprintf(`SELECT cluster_id, centroid_blob FROM %s_cluster_tree WHERE parent_cluster_id = ?`, vi.TableName),
			[]sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeInteger, Value: currentClusterID},
			},
		)

		if err != nil || len(childRes.Rows) == 0 {
			// No children - treat this as a leaf
			return currentClusterID, finalDistance, nil
		}

		// Find child with closest centroid
		var bestChild int64
		bestDistance := float64(1e9)

		for _, row := range childRes.Rows {
			childID := row[0].Int64()
			centroidBlob := row[1].Blob()

			centroid, err := ParseVectorBlob(centroidBlob)

			if err != nil {
				continue
			}

			distance := vi.calculateDistance(vector, centroid.GetFloat32Slice())

			if distance < bestDistance {
				bestDistance = distance
				bestChild = childID
			}
		}

		if bestChild == 0 {
			// Couldn't find a valid child, return current
			return currentClusterID, finalDistance, nil
		}

		// Move to best child
		currentClusterID = bestChild
	}
}

// getOrCreateRootCluster ensures root cluster exists and returns its ID
func (vi *VectorIndexer) getOrCreateRootCluster(db *database.DatabaseConnection) (int64, error) {
	// Try to get existing root
	res, err := db.Exec(
		fmt.Sprintf(`SELECT cluster_id FROM %s_cluster_tree WHERE cluster_id = 1`, vi.TableName),
		nil,
	)

	if err == nil && len(res.Rows) > 0 {
		return res.Rows[0][0].Int64(), nil
	}

	// Create root cluster with zero centroid
	zeroCentroid := make([]float32, vi.Dimensions)
	centroidBlob, err := EncodeFloat32(zeroCentroid)

	if err != nil {
		return 0, err
	}

	_, err = db.Exec(
		fmt.Sprintf(`INSERT INTO %s_cluster_tree (cluster_id, parent_id, centroid_blob, cluster_size, is_leaf, created_at) VALUES (?, NULL, ?, 0, 1, ?)`, vi.TableName),
		[]sqlite3.StatementParameter{
			{Type: sqlite3.ParameterTypeInteger, Value: int64(1)},
			{Type: sqlite3.ParameterTypeBlob, Value: centroidBlob}, {Type: sqlite3.ParameterTypeInteger, Value: time.Now().UTC().Unix()}},
	)

	if err != nil {
		return 0, err
	}

	return 1, nil
}

// calculateDistance calculates distance between two vectors based on metric
func (vi *VectorIndexer) calculateDistance(a, b []float32) float64 {
	switch vi.DistanceMetric {
	case 0: // L2
		sum := float64(0)

		for i := range a {
			diff := float64(a[i] - b[i])
			sum += diff * diff
		}

		return sum
	case 1: // Cosine
		dotProduct := float64(0)
		normA := float64(0)
		normB := float64(0)

		for i := range a {
			dotProduct += float64(a[i] * b[i])
			normA += float64(a[i] * a[i])
			normB += float64(b[i] * b[i])
		}

		return 1.0 - (dotProduct / (normA * normB))
	default:
		return float64(1e9)
	}
}

// updateClusterSize updates the size of a single cluster
func (vi *VectorIndexer) updateClusterSize(db *database.DatabaseConnection, clusterID int64, delta int) error {
	_, err := db.Exec(
		fmt.Sprintf(`UPDATE %s_cluster_tree SET cluster_size = cluster_size + ? WHERE cluster_id = ?`, vi.TableName),
		[]sqlite3.StatementParameter{
			{Type: sqlite3.ParameterTypeInteger, Value: int64(delta)},
			{Type: sqlite3.ParameterTypeInteger, Value: clusterID},
		},
	)

	return err
}

// getClusterCentroid retrieves the current centroid and size for a cluster
func (vi *VectorIndexer) getClusterCentroid(db *database.DatabaseConnection, clusterID int64) ([]float32, int, error) {
	res, err := db.Exec(
		fmt.Sprintf(`SELECT centroid_blob, cluster_size FROM %s_cluster_tree WHERE cluster_id = ?`, vi.TableName),
		[]sqlite3.StatementParameter{
			{Type: sqlite3.ParameterTypeInteger, Value: clusterID},
		},
	)

	if err != nil || len(res.Rows) == 0 {
		return nil, 0, fmt.Errorf("cluster not found")
	}

	centroidBlob := res.Rows[0][0].Blob()
	size := int(res.Rows[0][1].Int64())

	centroid, err := ParseVectorBlob(centroidBlob)

	if err != nil {
		return nil, 0, err
	}

	return centroid.GetFloat32Slice(), size, nil
}

// splitOversizedClusters checks all clusters and splits any that exceed MaxClusterSize
func (vi *VectorIndexer) splitOversizedClusters(ctx context.Context) error {
	// Keep splitting until no oversized clusters remain
	maxIterations := 10 // Prevent infinite loops

	for iteration := 0; iteration < maxIterations; iteration++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Find clusters that are too large
		// Use 1.5x MaxClusterSize threshold to avoid excessive splitting
		// This allows some overfill for better tree structure
		splitThreshold := int64(float64(vi.MaxClusterSize) * 1.5)

		res, err := vi.DB.Exec(
			fmt.Sprintf(`SELECT cluster_id, cluster_size FROM %s_cluster_tree WHERE cluster_size > ? AND is_leaf = 1`, vi.TableName),
			[]sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeInteger, Value: splitThreshold},
			},
		)

		if err != nil {
			return err
		}

		if len(res.Rows) == 0 {
			if iteration > 0 {
				slog.Info("Cluster splitting converged",
					"table", vi.TableName,
					"iterations", iteration,
					"max_size", vi.MaxClusterSize)
			}

			return nil // No more clusters need splitting
		}

		slog.Info("Splitting oversized clusters",
			"table", vi.TableName,
			"iteration", iteration+1,
			"count", len(res.Rows),
			"max_size", vi.MaxClusterSize)

		for _, row := range res.Rows {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			clusterID := row[0].Int64()
			clusterSize := int(row[1].Int64())

			slog.Debug("Splitting cluster",
				"cluster_id", clusterID,
				"size", clusterSize,
				"max_size", vi.MaxClusterSize)

			if err := vi.splitCluster(ctx, clusterID); err != nil {
				slog.Error("Failed to split cluster", "cluster_id", clusterID, "error", err)
				// Continue with other clusters
			}
		}
	}

	slog.Warn("Cluster splitting reached max iterations",
		"table", vi.TableName,
		"max_iterations", maxIterations)

	return nil
}

// splitCluster splits a single cluster into k child clusters using distance-based quantile split
func (vi *VectorIndexer) splitCluster(ctx context.Context, clusterID int64) error {
	// B+ tree structure: max 16 children per internal node, ~5000 vectors per leaf
	// Calculate k based on cluster size to target MaxClusterSize vectors per child
	// k = min(16, ceil(cluster_size / MaxClusterSize))
	// For 100k vectors: k = min(16, 100k/5k) = min(16, 20) = 16 -> 6250 per child
	// For 6250 vectors: k = min(16, 6250/5k) = min(16, 2) = 2 -> 3125 per child

	// First, we need to get the cluster size
	var clusterSize int64

	err := vi.DB.Transaction(false, func(db *database.DatabaseConnection) error {
		res, err := db.Exec(
			fmt.Sprintf(`SELECT cluster_size FROM %s_cluster_tree WHERE cluster_id = ?`, vi.TableName),
			[]sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeInteger, Value: clusterID},
			},
		)

		if err != nil || len(res.Rows) == 0 {
			return fmt.Errorf("failed to get cluster size: %w", err)
		}

		clusterSize = res.Rows[0][0].Int64()

		return nil
	})

	if err != nil {
		return err
	}

	// Calculate optimal k: enough children to keep each near MaxClusterSize
	// Don't clamp to 16 - we'll handle >16 children by splitting the internal node
	k := max(int(math.Ceil(float64(clusterSize)/float64(vi.MaxClusterSize))), 2)

	// Note: No max limit - if k > 16, we'll split the internal node later

	// Get vector column name first
	var vectorColumn string

	err = vi.DB.Transaction(false, func(db *database.DatabaseConnection) error {
		var err error

		vectorColumn, err = getVectorColumnName(db, vi.TableName)

		return err
	})

	if err != nil {
		return err
	}

	// Fetch vectors with their distances and actual vector data
	var vectors []struct {
		id       int64
		distance float64
		vector   []float32
	}

	err = vi.DB.Transaction(false, func(db *database.DatabaseConnection) error {
		vectorRes, err := db.Exec(
			fmt.Sprintf(`
				SELECT m.vector_id, IFNULL(m.distance, 0.0), v.%s
				FROM %s_cluster_vector_map m
				JOIN %s_vectors v ON v.rowid = m.vector_id
				WHERE m.cluster_id = ?
				ORDER BY IFNULL(m.distance, 0.0) ASC
				LIMIT 100000`,
				vectorColumn, vi.TableName, vi.TableName),
			[]sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeInteger, Value: clusterID},
			},
		)

		if err != nil {
			return err
		}

		if len(vectorRes.Rows) < k*2 {
			// Not enough vectors to split meaningfully
			return nil
		}

		vectors = make([]struct {
			id       int64
			distance float64
			vector   []float32
		}, 0, len(vectorRes.Rows))

		for _, row := range vectorRes.Rows {
			vectorID := row[0].Int64()
			distance := row[1].Float64()
			vectorBlob := row[2].Blob()

			// Parse vector
			vb, err := ParseVectorBlob(vectorBlob)

			if err != nil {
				slog.Error("Failed to parse vector blob during split", "id", vectorID, "error", err)
				continue
			}

			vectorData := vb.GetFloat32Slice()

			if len(vectorData) != vi.Dimensions {
				slog.Error("Vector dimension mismatch during split", "id", vectorID, "expected", vi.Dimensions, "got", len(vectorData))
				continue
			}

			vectors = append(vectors, struct {
				id       int64
				distance float64
				vector   []float32
			}{
				id:       vectorID,
				distance: distance,
				vector:   vectorData,
			})
		}

		return nil
	})

	if err != nil || len(vectors) == 0 {
		return err
	}

	// Split vectors by distance into k equal-sized quantiles (B+ tree style)
	// Vectors are already sorted by distance from closest to furthest
	// For k=8: divide into 8 equal chunks (octiles)
	// For k=16: divide into 16 equal chunks
	vectorsPerChild := len(vectors) / k
	assignments := make([]int, len(vectors))

	for i := 0; i < len(vectors); i++ {
		// Assign to quantile based on position in sorted list
		childIdx := i / vectorsPerChild

		// Handle remainder vectors - assign to last child
		if childIdx >= k {
			childIdx = k - 1
		}

		assignments[i] = childIdx
	}

	// Compute centroids for each child cluster
	childCentroids := make([][]float32, k)

	for i := 0; i < k; i++ {
		centroid := make([]float32, vi.Dimensions)
		count := 0

		for j, v := range vectors {
			if assignments[j] == i {
				for dim := 0; dim < vi.Dimensions; dim++ {
					centroid[dim] += v.vector[dim]
				}
				count++
			}
		}

		// Compute mean
		if count > 0 {
			for dim := 0; dim < vi.Dimensions; dim++ {
				centroid[dim] /= float32(count)
			}
		}

		childCentroids[i] = centroid
	}

	// Now apply the clustering results in a transaction
	return vi.DB.Transaction(false, func(db *database.DatabaseConnection) error {

		// Get next cluster ID
		maxIDRes, err := db.Exec(
			fmt.Sprintf(`SELECT IFNULL(MAX(cluster_id), 0) FROM %s_cluster_tree`, vi.TableName),
			nil,
		)

		if err != nil {
			return err
		}

		nextClusterID := maxIDRes.Rows[0][0].Int64() + 1

		// Create child clusters with computed centroids
		now := time.Now().UTC().Unix()

		for i := 0; i < k; i++ {
			childClusterID := nextClusterID + int64(i)

			// Encode centroid using same format as vectors
			centroidBlob := encodeFloat32Vector(childCentroids[i])

			// Insert child cluster with computed centroid
			_, err = db.Exec(
				fmt.Sprintf(`INSERT INTO %s_cluster_tree (cluster_id, parent_id, centroid_blob, cluster_size, is_leaf, created_at) VALUES (?, ?, ?, 0, 1, ?)`, vi.TableName),
				[]sqlite3.StatementParameter{
					{Type: sqlite3.ParameterTypeInteger, Value: childClusterID},
					{Type: sqlite3.ParameterTypeInteger, Value: clusterID},
					{Type: sqlite3.ParameterTypeBlob, Value: centroidBlob},
					{Type: sqlite3.ParameterTypeInteger, Value: now},
				},
			)

			if err != nil {
				return err
			}
		}

		// Update parent cluster to be non-leaf
		_, err = db.Exec(
			fmt.Sprintf(`UPDATE %s_cluster_tree SET is_leaf = 0 WHERE cluster_id = ?`, vi.TableName),
			[]sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeInteger, Value: clusterID},
			},
		)

		if err != nil {
			return err
		}

		// DELETE old parent cluster mappings first to avoid duplicate mappings
		// This is critical - without this, vectors would be mapped to BOTH parent and children
		_, err = db.Exec(
			fmt.Sprintf(`DELETE FROM %s_cluster_vector_map WHERE cluster_id = ?`, vi.TableName),
			[]sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeInteger, Value: clusterID},
			},
		)

		if err != nil {
			return err
		}

		// Reassign vectors to child clusters in batches
		// SQLite has a limit on the number of parameters (default 999, max 32766)
		// With 4 params per row (vector_id, cluster_id, distance, indexed_at),
		// we can safely do 8000 rows per chunk (32000 parameters)
		const batchSize = 8000

		for i := 0; i < len(vectors); i += batchSize {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			end := i + batchSize

			if end > len(vectors) {
				end = len(vectors)
			}

			batch := vectors[i:end]
			valuesParts := make([]string, 0, len(batch))
			params := make([]sqlite3.StatementParameter, 0, len(batch)*4)

			clusterSizes := make(map[int64]int)

			for j, v := range batch {
				childClusterID := nextClusterID + int64(assignments[i+j])
				valuesParts = append(valuesParts, "(?, ?, ?, ?)")
				params = append(params,
					sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: v.id},
					sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: childClusterID},
					sqlite3.StatementParameter{Type: sqlite3.ParameterTypeFloat, Value: v.distance},
					sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: now},
				)
				clusterSizes[childClusterID]++
			}

			query := fmt.Sprintf(
				`INSERT INTO %s_cluster_vector_map (vector_id, cluster_id, distance, indexed_at) VALUES %s`,
				vi.TableName,
				strings.Join(valuesParts, ", "),
			)

			if _, err := db.Exec(query, params); err != nil {
				return err
			}

			// Update cluster sizes
			for childID, count := range clusterSizes {
				if err := vi.updateClusterSize(db, childID, count); err != nil {
					slog.Error("Failed to update child cluster size", "cluster", childID, "error", err)
				}
			}
		}

		// Update parent cluster size to 0 (vectors moved to children)
		_, err = db.Exec(
			fmt.Sprintf(`UPDATE %s_cluster_tree SET cluster_size = 0 WHERE cluster_id = ?`, vi.TableName),
			[]sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeInteger, Value: clusterID},
			},
		)

		if err != nil {
			return err
		}

		// If we created more than 16 children, split the internal node
		if k > 16 {
			err = vi.splitInternalNode(db, clusterID, nextClusterID, k)

			if err != nil {
				return fmt.Errorf("failed to split internal node: %w", err)
			}
		}

		slog.Info("Cluster split completed",
			"cluster_id", clusterID,
			"children", k,
			"vectors_reassigned", len(vectors))

		return nil
	})
}

// splitInternalNode splits an internal node that has too many children (>16)
// into multiple internal nodes, each with at most 16 children
func (vi *VectorIndexer) splitInternalNode(db *database.DatabaseConnection, parentClusterID, firstChildID int64, totalChildren int) error {
	// Calculate how many internal nodes we need
	// Each internal node can have up to 16 children
	const maxChildrenPerNode = 16

	numInternalNodes := (totalChildren + maxChildrenPerNode - 1) / maxChildrenPerNode

	if numInternalNodes <= 1 {
		// No need to split if we have 16 or fewer children
		return nil
	}

	// Get next available cluster ID for new internal nodes
	maxIDRes, err := db.Exec(
		fmt.Sprintf(`SELECT IFNULL(MAX(cluster_id), 0) FROM %s_cluster_tree`, vi.TableName),
		nil,
	)

	if err != nil {
		return err
	}

	newInternalNodeID := maxIDRes.Rows[0][0].Int64() + 1

	now := time.Now().UTC().Unix()

	// First, compute centroids for each group of children
	// We need this before creating the internal nodes because centroid_blob is NOT NULL
	internalNodeCentroids := make([][]byte, numInternalNodes)

	for i := range numInternalNodes {
		// Calculate which children belong to this internal node
		startIdx := i * maxChildrenPerNode
		endIdx := min((i+1)*maxChildrenPerNode, totalChildren)

		// Get centroids of children that will belong to this internal node
		centroid := make([]float32, vi.Dimensions)
		count := 0

		for j := startIdx; j < endIdx; j++ {
			childID := firstChildID + int64(j)

			// Get this child's centroid
			childRes, err := db.Exec(
				fmt.Sprintf(`SELECT centroid_blob FROM %s_cluster_tree WHERE cluster_id = ?`, vi.TableName),
				[]sqlite3.StatementParameter{
					{Type: sqlite3.ParameterTypeInteger, Value: childID},
				},
			)

			if err != nil || len(childRes.Rows) == 0 {
				continue
			}

			centroidBlob := childRes.Rows[0][0].Blob()

			if len(centroidBlob) == 0 {
				continue
			}

			vb, err := ParseVectorBlob(centroidBlob)

			if err != nil {
				continue
			}

			childCentroid := vb.GetFloat32Slice()

			if len(childCentroid) != vi.Dimensions {
				continue
			}

			for dim := 0; dim < vi.Dimensions; dim++ {
				centroid[dim] += childCentroid[dim]
			}

			count++
		}

		// Compute mean
		if count > 0 {
			for dim := 0; dim < vi.Dimensions; dim++ {
				centroid[dim] /= float32(count)
			}
		}

		internalNodeCentroids[i] = encodeFloat32Vector(centroid)
	}

	// Create new internal nodes with computed centroids (one less than numInternalNodes, since the original parent becomes one)
	for i := 1; i < numInternalNodes; i++ {
		_, err = db.Exec(
			fmt.Sprintf(`INSERT INTO %s_cluster_tree (cluster_id, parent_id, centroid_blob, cluster_size, is_leaf, created_at) VALUES (?, ?, ?, 0, 0, ?)`, vi.TableName),
			[]sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeInteger, Value: newInternalNodeID + int64(i) - 1},
				{Type: sqlite3.ParameterTypeInteger, Value: parentClusterID},
				{Type: sqlite3.ParameterTypeBlob, Value: internalNodeCentroids[i]},
				{Type: sqlite3.ParameterTypeInteger, Value: now},
			},
		)

		if err != nil {
			return err
		}
	}

	// Update the original parent's centroid
	_, err = db.Exec(
		fmt.Sprintf(`UPDATE %s_cluster_tree SET centroid_blob = ? WHERE cluster_id = ?`, vi.TableName),
		[]sqlite3.StatementParameter{
			{Type: sqlite3.ParameterTypeBlob, Value: internalNodeCentroids[0]},
			{Type: sqlite3.ParameterTypeInteger, Value: parentClusterID},
		},
	)

	if err != nil {
		return err
	}

	// Reassign children to internal nodes in groups of up to 16
	// First group stays with original parent, rest go to new internal nodes
	for i := range totalChildren {
		childID := firstChildID + int64(i)
		internalNodeIdx := i / maxChildrenPerNode

		// First group (0-15) stays with original parent
		if internalNodeIdx == 0 {
			continue
		}

		// Subsequent groups get reassigned to new internal nodes
		newParentID := newInternalNodeID + int64(internalNodeIdx) - 1

		_, err = db.Exec(
			fmt.Sprintf(`UPDATE %s_cluster_tree SET parent_id = ? WHERE cluster_id = ?`, vi.TableName),
			[]sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeInteger, Value: newParentID},
				{Type: sqlite3.ParameterTypeInteger, Value: childID},
			},
		)

		if err != nil {
			return err
		}
	}

	return nil
}

// kMeans performs k-means clustering on a set of vectors
func (vi *VectorIndexer) kMeans(ctx context.Context, vectors []struct {
	id     int64
	vector []float32
}, k int) ([]int, [][]float32, error) {
	if len(vectors) < k {
		return nil, nil, fmt.Errorf("not enough vectors for k-means: %d < %d", len(vectors), k)
	}

	dims := len(vectors[0].vector)

	// Initialize centroids using k-means++ for better convergence
	centroids := make([][]float32, k)
	centroids[0] = make([]float32, dims)
	copy(centroids[0], vectors[0].vector)

	for i := 1; i < k; i++ {
		// Find vector farthest from existing centroids
		maxDist := float64(0)
		maxIdx := 0

		for j, v := range vectors {
			minDist := float64(1e9)

			for c := 0; c < i; c++ {
				dist := vi.calculateDistance(v.vector, centroids[c])

				if dist < minDist {
					minDist = dist
				}
			}

			if minDist > maxDist {
				maxDist = minDist
				maxIdx = j
			}
		}

		centroids[i] = make([]float32, dims)
		copy(centroids[i], vectors[maxIdx].vector)
	}

	// K-means iterations
	assignments := make([]int, len(vectors))
	maxIterations := 10

	for iter := 0; iter < maxIterations; iter++ {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		default:
		}

		changed := false

		// Assignment step
		for i, v := range vectors {
			bestCluster := 0
			bestDist := vi.calculateDistance(v.vector, centroids[0])

			for c := 1; c < k; c++ {
				dist := vi.calculateDistance(v.vector, centroids[c])

				if dist < bestDist {
					bestDist = dist
					bestCluster = c
				}
			}

			if assignments[i] != bestCluster {
				changed = true
				assignments[i] = bestCluster
			}
		}

		if !changed {
			break // Converged
		}

		// Update centroids
		clusterSums := make([][]float64, k)
		clusterCounts := make([]int, k)

		for i := 0; i < k; i++ {
			clusterSums[i] = make([]float64, dims)
		}

		for i, v := range vectors {
			cluster := assignments[i]
			clusterCounts[cluster]++

			for d := 0; d < dims; d++ {
				clusterSums[cluster][d] += float64(v.vector[d])
			}
		}

		for c := 0; c < k; c++ {
			if clusterCounts[c] > 0 {
				for d := 0; d < dims; d++ {
					centroids[c][d] = float32(clusterSums[c][d] / float64(clusterCounts[c]))
				}
			}
		}
	}

	return assignments, centroids, nil
}

// encodeFloat32Vector encodes a float32 vector into the VectorBlob format
func encodeFloat32Vector(vec []float32) []byte {
	const (
		vectorVersion1 = 0x01
		vectorTypeF32  = 0x01
	)

	// Calculate total size: version + type + dimensions + data
	blobSize := 1 + 1 + 4 + len(vec)*4
	blob := make([]byte, blobSize)

	// Version byte
	blob[0] = vectorVersion1

	// Type byte (float32)
	blob[1] = vectorTypeF32

	// Dimensions (uint32, little-endian)
	dims := uint32(len(vec))
	blob[2] = byte(dims)
	blob[3] = byte(dims >> 8)
	blob[4] = byte(dims >> 16)
	blob[5] = byte(dims >> 24)

	// Vector data (each float32 is 4 bytes, little-endian)
	offset := 6

	for _, val := range vec {
		bits := math.Float32bits(val)
		blob[offset] = byte(bits)
		blob[offset+1] = byte(bits >> 8)
		blob[offset+2] = byte(bits >> 16)
		blob[offset+3] = byte(bits >> 24)
		offset += 4
	}

	return blob
}
