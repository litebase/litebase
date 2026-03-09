package database

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"

	"github.com/litebase/litebase/pkg/sqlite3"
	"github.com/litebase/litebase/pkg/vector"
)

// VectorColumnInfo contains metadata about a vector column
type VectorColumnInfo struct {
	Name           string
	Dimensions     int
	DistanceMetric int
	Index          int // Column index in schema
}

// GetVectorColumns queries the metadata table to get all vector column definitions (exported for use in server package)
func GetVectorColumns(db *DatabaseConnection, tableName string) ([]VectorColumnInfo, error) {
	return getVectorColumns(db, tableName)
}

// getVectorColumns queries the metadata table to get all vector column definitions
func getVectorColumns(db *DatabaseConnection, tableName string) ([]VectorColumnInfo, error) {
	// Fetch ALL metadata in a single query to avoid N+1 queries
	res, err := db.Exec(
		fmt.Sprintf("SELECT key, value FROM %s_metadata WHERE key LIKE 'column_%%' OR key = 'column_count' ORDER BY key", tableName),
		nil,
	)

	if res != nil {
		defer db.ResultPool().Put(res)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to query metadata: %w", err)
	}

	if len(res.Rows) == 0 {
		return nil, fmt.Errorf("no metadata found")
	}

	// Parse all metadata into a map for efficient lookup
	metadata := make(map[string]string)

	for _, row := range res.Rows {
		if len(row) < 2 {
			continue
		}

		key := string(row[0].Text())
		value := string(row[1].Text())
		metadata[key] = value
	}

	// Get column count
	columnCountStr, ok := metadata["column_count"]

	if !ok {
		return nil, fmt.Errorf("no column_count metadata found")
	}

	var columnCount int
	fmt.Sscanf(columnCountStr, "%d", &columnCount)

	vectorColumns := make([]VectorColumnInfo, 0)

	// Parse each column's metadata from the map
	for i := 0; i < columnCount; i++ {
		colName, hasName := metadata[fmt.Sprintf("column_%d_name", i)]

		if !hasName {
			continue
		}

		colType, hasType := metadata[fmt.Sprintf("column_%d_type", i)]

		if !hasType || colType != "BLOB" {
			continue // Only process vector (BLOB) columns
		}

		dimensionsStr, hasDims := metadata[fmt.Sprintf("column_%d_dimensions", i)]

		if !hasDims {
			continue
		}

		var dimensions int
		fmt.Sscanf(dimensionsStr, "%d", &dimensions)

		metricStr, hasMetric := metadata[fmt.Sprintf("column_%d_distance_metric", i)]

		if !hasMetric {
			continue
		}

		var distanceMetric int
		fmt.Sscanf(metricStr, "%d", &distanceMetric)

		vectorColumns = append(vectorColumns, VectorColumnInfo{
			Name:           colName,
			Dimensions:     dimensions,
			DistanceMetric: distanceMetric,
			Index:          i,
		})
	}

	if len(vectorColumns) == 0 {
		return nil, fmt.Errorf("no vector columns found in metadata")
	}

	return vectorColumns, nil
}

// ClusterNode represents a node in the cluster tree (for in-memory traversal)
type ClusterNode struct {
	ClusterID   int64
	ParentID    *int64
	Centroid    []float32
	IsLeaf      bool
	ClusterSize int
	Children    []int64 // Child cluster IDs
}

// VectorIndexer processes vectors in cluster 0 and reassigns them to proper clusters
type VectorIndexer struct {
	DB             *DatabaseConnection
	TableName      string
	VectorColumns  []VectorColumnInfo // All vector columns with their config
	MaxClusterSize int
	MinClusterSize int
}

var statementParamsPool = sync.Pool{
	New: func() interface{} { return make([]sqlite3.StatementParameter, 10000*3) },
}

// NewVectorIndexer creates a new vector indexer
func NewVectorIndexer(db *DatabaseConnection, tableName string, vectorColumns []VectorColumnInfo, maxClusterSize, minClusterSize int) (*VectorIndexer, error) {
	return &VectorIndexer{
		DB:             db,
		TableName:      tableName,
		VectorColumns:  vectorColumns,
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

	var totalProcessed int

	// Distribute batch size across all vector columns
	perColumnBatch := max(batchSize/len(vi.VectorColumns), 1)

	// Process each vector column independently
	for colIdx, colInfo := range vi.VectorColumns {
		// Give remainder to last column
		colBatchSize := perColumnBatch

		if colIdx == len(vi.VectorColumns)-1 {
			colBatchSize = batchSize - (perColumnBatch * (len(vi.VectorColumns) - 1))
		}

		var processed int

		err := vi.DB.Transaction(false, func(db *DatabaseConnection) error {
			// Check context again inside transaction
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			// Get vectors currently assigned to cluster 0 (need reassignment) for this column
			var selQB strings.Builder
			selQB.Grow(128)
			selQB.WriteString("SELECT v.id, v.")
			selQB.WriteString(colInfo.Name)
			selQB.WriteString(" FROM ")
			selQB.WriteString(vi.TableName)
			selQB.WriteString("_vectors v INNER JOIN ")
			selQB.WriteString(vi.TableName)
			selQB.WriteString("_")
			selQB.WriteString(colInfo.Name)
			selQB.WriteString("_cluster_vector_map m ON v.id = m.vector_id WHERE m.cluster_id = 0 ORDER BY v.rowid ASC LIMIT ?")

			// Pre-load cluster tree into memory to avoid N+1 queries
			clusterTree, err := vi.loadClusterTree(db, colInfo.Name)

			if err != nil {
				return fmt.Errorf("failed to load cluster tree: %w", err)
			}

			// Track cluster updates
			clusterSizeDeltas := make(map[int64]int)       // cluster_id -> size change
			clusterVectorSums := make(map[int64][]float32) // cluster_id -> sum of vectors for centroid update

			// Use a preallocated slice for assignments to avoid per-row map allocations
			assignments := make([]struct {
				vectorID  int64
				clusterID int64
				distance  float64
			}, 0, colBatchSize)

			foundCount := 0

			// Stream rows to avoid buffering large result sets in memory
			err = db.ExecStream(
				selQB.String(),
				[]sqlite3.StatementParameter{{Type: "INTEGER", Value: int64(colBatchSize)}},
				func(row []*sqlite3.Column) error {
					// Check context during loop processing
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
					}

					foundCount++

					if len(row) < 2 {
						return nil
					}

					vectorID := row[0].Int64()
					vectorBlob := row[1].Blob()

					// Parse vector using pooled VectorBlob to reduce allocations
					vb, err := vector.ParseVectorBlobPooled(vectorBlob)

					if err != nil {
						slog.Error("Failed to parse vector blob",
							"id", vectorID,
							"column", colInfo.Name,
							"blob_size", len(vectorBlob),
							"error", err)
						return nil
					}

					vec := vb.GetFloat32Slice()

					if len(vec) != colInfo.Dimensions {
						slog.Error("Vector dimension mismatch", "id", vectorID, "expected", colInfo.Dimensions, "got", len(vec))
						vector.PutVectorBlob(vb)
						return nil
					}

					// Find best cluster using in-memory hierarchical traversal (no DB queries)
					clusterID, distance := vi.findBestClusterInMemory(clusterTree, colInfo.DistanceMetric, vec)

					if clusterID == 0 {
						slog.Error("Failed to find best cluster", "id", vectorID)
						vector.PutVectorBlob(vb)
						return nil
					}

					// Record assignment and distance (append to slice to avoid map allocations)
					assignments = append(assignments, struct {
						vectorID  int64
						clusterID int64
						distance  float64
					}{vectorID, clusterID, distance})
					clusterSizeDeltas[clusterID]++

					// Accumulate vector for centroid update
					if _, exists := clusterVectorSums[clusterID]; !exists {
						clusterVectorSums[clusterID] = make([]float32, len(vec))
					}

					for i := range vec {
						clusterVectorSums[clusterID][i] += vec[i]
					}

					// Return pooled VectorBlob immediately to avoid allocations
					vector.PutVectorBlob(vb)

					return nil
				},
			)

			if err != nil {
				return fmt.Errorf("failed to query vectors in cluster 0: %w", err)
			}

			totalDelta := 0

			for _, d := range clusterSizeDeltas {
				totalDelta += d
			}

			if foundCount == 0 {
				// No vectors to reassign
				return nil
			}

			// No result buffer to return when streaming

			// Update cluster mappings in batch
			if len(assignments) > 0 {
				// SQLite has a limit on the number of parameters (default 999, max 32766)
				// With 3 params per row (vector_id, cluster_id, distance),
				// we can safely do 10000 rows per chunk (30000 parameters)
				const maxRowsPerBatch = 10000

				// assignments already collected in the loop above (slice), reuse it

				// Process in chunks
				// Reuse a parameter buffer from the pool to avoid allocating a new slice each chunk
				paramsBuf := statementParamsPool.Get().([]sqlite3.StatementParameter)

				for i := 0; i < len(assignments); i += maxRowsPerBatch {
					end := min(i+maxRowsPerBatch, len(assignments))

					chunk := assignments[i:end]
					// Build bulk UPDATE statement for this chunk using a single Builder
					params := paramsBuf[:len(chunk)*3]

					var vbldr strings.Builder
					// Reserve approximate size: 12 bytes per row is enough for "(?, ?, ?), "
					vbldr.Grow(len(chunk) * 12)

					p := 0

					for idx, assignment := range chunk {
						vbldr.WriteString("(?, ?, ?)")

						if idx != len(chunk)-1 {
							vbldr.WriteString(", ")
						}

						params[p] = sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: assignment.vectorID}
						p++
						params[p] = sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: assignment.clusterID}
						p++
						params[p] = sqlite3.StatementParameter{Type: sqlite3.ParameterTypeFloat, Value: assignment.distance}
						p++
					}

					// Build query without fmt.Sprintf to avoid allocation
					var qbldr strings.Builder
					qbldr.Grow(len(vi.TableName) + len(colInfo.Name) + vbldr.Len() + 100)
					qbldr.WriteString("INSERT OR REPLACE INTO ")
					qbldr.WriteString(vi.TableName)
					qbldr.WriteString("_")
					qbldr.WriteString(colInfo.Name)
					qbldr.WriteString("_cluster_vector_map (vector_id, cluster_id, distance) VALUES ")
					qbldr.WriteString(vbldr.String())

					insRes, err := db.Exec(qbldr.String(), params)

					if insRes != nil {
						db.ResultPool().Put(insRes)
					}

					if err != nil {
						return err
					}
				}

				// Decrement cluster 0 size
				cluster0Delta := -len(assignments)

				if err := vi.updateClusterSize(db, colInfo.Name, 0, cluster0Delta); err != nil {
					slog.Error("Failed to update cluster 0 size", "error", err)
				}

				// Increment target cluster sizes
				for clusterID, delta := range clusterSizeDeltas {
					if err := vi.updateClusterSize(db, colInfo.Name, clusterID, delta); err != nil {
						slog.Error("Failed to update cluster size", "cluster", clusterID, "error", err)
					}
				}
			}

			// Update cluster centroids for this column
			for clusterID, vectorSum := range clusterVectorSums {
				count := clusterSizeDeltas[clusterID]

				if count == 0 {
					continue
				}

				// Get current centroid and size
				currentCentroid, currentSize, err := vi.getClusterCentroid(db, colInfo.Name, clusterID)

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
				centroidBlob, err := vector.EncodeFloat32(newCentroid)

				if err != nil {
					slog.Error("Failed to serialize centroid", "cluster", clusterID, "error", err)
					continue
				}

				// Update centroid in {table}_{column}_cluster_tree
				var uqb strings.Builder
				uqb.Grow(len(vi.TableName) + len(colInfo.Name) + 80)
				uqb.WriteString("UPDATE ")
				uqb.WriteString(vi.TableName)
				uqb.WriteString("_")
				uqb.WriteString(colInfo.Name)
				uqb.WriteString("_cluster_tree SET centroid_blob = ? WHERE cluster_id = ?")

				updRes, err := db.Exec(
					uqb.String(),
					[]sqlite3.StatementParameter{
						{Type: sqlite3.ParameterTypeBlob, Value: centroidBlob},
						{Type: sqlite3.ParameterTypeInteger, Value: clusterID},
					},
				)

				if updRes != nil {
					db.ResultPool().Put(updRes)
				}

				if err != nil {
					slog.Error("Failed to update centroid", "cluster", clusterID, "error", err)
				}
			}

			// Consider rows examined as processed so the indexer continues
			// when rows were found but none produced assignments (e.g. parse errors).
			if len(assignments) == 0 && foundCount > 0 {
				slog.Warn("No assignments generated for scanned rows; will retry",
					"table", vi.TableName,
					"column", colInfo.Name,
					"scanned", foundCount)
			}

			// Only count successful assignments as processed; the caller will
			// re-check cluster-0 to decide whether more processing is needed.
			processed = len(assignments)

			return nil
		})

		if err != nil {
			return totalProcessed, err
		}

		totalProcessed += processed

		// After successful batch processing, check if any clusters need splitting for this column
		// This happens OUTSIDE the transaction to avoid long-running locks
		if processed > 0 && vi.MaxClusterSize > 0 {
			select {
			case <-ctx.Done():
				return totalProcessed, ctx.Err()
			default:
			}

			if err := vi.splitOversizedClusters(ctx, colInfo.Name, colInfo.DistanceMetric); err != nil {
				slog.Error("Failed to split oversized clusters", "column", colInfo.Name, "error", err)
				// Don't fail the batch - splitting can be retried later
			}
		}
	}

	return totalProcessed, nil
}

// loadClusterTree loads the entire cluster tree for a column into memory
func (vi *VectorIndexer) loadClusterTree(db *DatabaseConnection, columnName string) (map[int64]*ClusterNode, error) {
	var treeQB strings.Builder
	treeQB.Grow(128)
	treeQB.WriteString("SELECT cluster_id, parent_id, centroid_blob, is_leaf, cluster_size FROM ")
	treeQB.WriteString(vi.TableName)
	treeQB.WriteString("_")
	treeQB.WriteString(columnName)
	treeQB.WriteString("_cluster_tree")

	res, err := db.Exec(
		treeQB.String(),
		nil,
	)

	if res != nil {
		defer db.ResultPool().Put(res)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to query cluster tree: %w", err)
	}

	if len(res.Rows) == 0 {
		return nil, fmt.Errorf("no clusters found in tree")
	}

	clusterTree := make(map[int64]*ClusterNode, len(res.Rows))

	for _, row := range res.Rows {
		if len(row) < 5 {
			return nil, fmt.Errorf("invalid cluster tree row: expected 5 columns, got %d", len(row))
		}

		clusterID := row[0].Int64()
		var parentID *int64

		if row[1].ColumnType == sqlite3.ColumnTypeInteger && len(row[1].ColumnValue) > 0 {
			pid := row[1].Int64()
			parentID = &pid
		}

		centroidBlob := row[2].Blob()

		if row[3].ColumnType != sqlite3.ColumnTypeInteger || len(row[3].ColumnValue) == 0 {
			slog.Warn("Invalid is_leaf value", "cluster_id", clusterID, "type", row[3].ColumnType, "len", len(row[3].ColumnValue))
			continue
		}

		isLeaf := row[3].Int64() == 1

		if row[4].ColumnType != sqlite3.ColumnTypeInteger || len(row[4].ColumnValue) == 0 {
			slog.Warn("Invalid cluster_size value", "cluster_id", clusterID, "type", row[4].ColumnType, "len", len(row[4].ColumnValue))
			continue
		}

		clusterSize := int(row[4].Int64())

		// Parse centroid
		var centroid []float32

		if len(centroidBlob) > 0 {
			vb, err := vector.ParseVectorBlob(centroidBlob)

			if err == nil {
				centroid = vb.GetFloat32Slice()
			}
		}

		clusterTree[clusterID] = &ClusterNode{
			ClusterID:   clusterID,
			ParentID:    parentID,
			Centroid:    centroid,
			IsLeaf:      isLeaf,
			ClusterSize: clusterSize,
			Children:    make([]int64, 0),
		}
	}

	// Build parent-child relationships
	for clusterID, node := range clusterTree {
		if node.ParentID != nil {
			if parent, exists := clusterTree[*node.ParentID]; exists {
				parent.Children = append(parent.Children, clusterID)
			}
		}
	}

	return clusterTree, nil
}

// findBestClusterInMemory finds the best leaf cluster using in-memory tree traversal
// Returns the cluster ID and distance (0 if tree is empty)
func (vi *VectorIndexer) findBestClusterInMemory(clusterTree map[int64]*ClusterNode, distanceMetric int, vector []float32) (int64, float64) {
	return FindBestClusterInMemory(clusterTree, distanceMetric, vector)
}

// FindBestClusterInMemory is the package-level version used by the inline CGo assignment path.
func FindBestClusterInMemory(clusterTree map[int64]*ClusterNode, distanceMetric int, vector []float32) (int64, float64) {
	// Start from root (cluster_id = 1)
	if len(clusterTree) == 0 {
		return 1, 0 // Default to root if tree is empty
	}

	currentNode, exists := clusterTree[1]

	if !exists {
		return 1, 0 // Default to root if not found
	}

	finalDistance := float64(0)

	for {
		// Calculate distance to current cluster's centroid
		if len(currentNode.Centroid) > 0 {
			finalDistance = calculateDistance(vector, currentNode.Centroid, distanceMetric)
		}

		if currentNode.IsLeaf {
			// Found a leaf cluster
			return currentNode.ClusterID, finalDistance
		}

		// Not a leaf - find best child cluster
		if len(currentNode.Children) == 0 {
			// No children - treat as leaf
			return currentNode.ClusterID, finalDistance
		}

		var bestChild *ClusterNode
		bestDistance := float64(1e9)

		for _, childID := range currentNode.Children {
			child, exists := clusterTree[childID]

			if !exists || len(child.Centroid) == 0 {
				continue
			}

			distance := calculateDistance(vector, child.Centroid, distanceMetric)

			if distance < bestDistance {
				bestDistance = distance
				bestChild = child
			}
		}

		if bestChild == nil {
			// Couldn't find valid child, return current
			return currentNode.ClusterID, finalDistance
		}

		// Move to best child
		currentNode = bestChild
	}
}

// getOrCreateRootCluster ensures root cluster exists and returns its ID
func (vi *VectorIndexer) getOrCreateRootCluster(db *DatabaseConnection, columnName string) (int64, error) {
	// Root cluster is always created by C code during table creation
	// Just verify it exists
	res, err := db.Exec(
		fmt.Sprintf(`SELECT cluster_id FROM %s_%s_cluster_tree WHERE cluster_id = 1`, vi.TableName, columnName),
		nil,
	)

	if res != nil {
		defer db.ResultPool().Put(res)
	}

	if err == nil && len(res.Rows) > 0 {
		return res.Rows[0][0].Int64(), nil
	}

	// If root doesn't exist, something is wrong
	return 0, fmt.Errorf("root cluster not found for column %s", columnName)
}

// calculateDistance calculates distance between two vectors based on metric
// calculateDistance computes the distance between two vectors using the specified metric
func calculateDistance(a, b []float32, distanceMetric int) float64 {
	return CalculateDistance(a, b, distanceMetric)
}

// CalculateDistance is the package-level version used by the inline CGo assignment path.
func CalculateDistance(a, b []float32, distanceMetric int) float64 {
	switch distanceMetric {
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

// updateClusterSize updates the size of a single cluster for a specific column
func (vi *VectorIndexer) updateClusterSize(db *DatabaseConnection, columnName string, clusterID int64, delta int) error {
	var qb strings.Builder
	qb.Grow(64)
	qb.WriteString("UPDATE ")
	qb.WriteString(vi.TableName)
	qb.WriteString("_")
	qb.WriteString(columnName)
	qb.WriteString("_cluster_tree SET cluster_size = cluster_size + ? WHERE cluster_id = ?")

	res, err := db.Exec(
		qb.String(),
		[]sqlite3.StatementParameter{
			{Type: sqlite3.ParameterTypeInteger, Value: int64(delta)},
			{Type: sqlite3.ParameterTypeInteger, Value: clusterID},
		},
	)

	if res != nil {
		db.ResultPool().Put(res)
	}

	return err
}

// getClusterCentroid retrieves the current centroid and size for a cluster in a specific column
func (vi *VectorIndexer) getClusterCentroid(db *DatabaseConnection, columnName string, clusterID int64) ([]float32, int, error) {
	var qb strings.Builder
	qb.Grow(96)
	qb.WriteString("SELECT centroid_blob, cluster_size FROM ")
	qb.WriteString(vi.TableName)
	qb.WriteString("_")
	qb.WriteString(columnName)
	qb.WriteString("_cluster_tree WHERE cluster_id = ?")

	res, err := db.Exec(
		qb.String(),
		[]sqlite3.StatementParameter{
			{Type: sqlite3.ParameterTypeInteger, Value: clusterID},
		},
	)

	if res != nil {
		defer db.ResultPool().Put(res)
	}

	if err != nil || len(res.Rows) == 0 {
		return nil, 0, fmt.Errorf("cluster not found")
	}

	centroidBlob := res.Rows[0][0].Blob()
	size := int(res.Rows[0][1].Int64())

	// Determine expected dimensions for this column
	dimensions := 0

	for _, col := range vi.VectorColumns {
		if col.Name == columnName {
			dimensions = col.Dimensions
			break
		}
	}

	// If no centroid blob or dimensions unknown, return zero vector
	if len(centroidBlob) == 0 || dimensions == 0 {
		// Return zeroed centroid of correct length when possible
		if dimensions > 0 {
			return make([]float32, dimensions), size, nil
		}

		return nil, size, nil
	}

	centroid, err := vector.ParseVectorBlob(centroidBlob)

	if err != nil {
		// Log and return zero centroid instead of failing - tolerate legacy/invalid blobs
		slog.Warn("Invalid centroid blob, using zero centroid", "cluster", clusterID, "column", columnName, "error", err)

		if dimensions > 0 {
			return make([]float32, dimensions), size, nil
		}

		return nil, size, nil
	}

	vec := centroid.GetFloat32Slice()

	// If parsed centroid length mismatches expected dimensions, pad/truncate as needed
	if dimensions > 0 && len(vec) != dimensions {
		newVec := make([]float32, dimensions)
		copy(newVec, vec)
		vec = newVec
	}

	return vec, size, nil
}

// SplitOversizedClusters is the exported wrapper used by the inline path.
func (vi *VectorIndexer) SplitOversizedClusters(ctx context.Context, columnName string, distanceMetric int) error {
	return vi.splitOversizedClusters(ctx, columnName, distanceMetric)
}

// splitOversizedClusters checks all clusters and splits any that exceed MaxClusterSize
func (vi *VectorIndexer) splitOversizedClusters(ctx context.Context, columnName string, distanceMetric int) error {
	// Enlarge the per-connection page cache for the duration of cluster splitting.
	// PRAGMA cache_size is connection-local and has no effect on WAL mode, other
	// connections, or checkpoint behaviour.  Each splitCluster call runs a write
	// transaction that repeatedly reads centroid blobs and updates cluster_tree
	// rows; keeping those B-tree pages warm across iterations reduces VFS
	// round-trips.  Restored unconditionally via defer.
	pragmaSetRes, pragmaSetErr := vi.DB.Exec("PRAGMA cache_size = -65536", nil)

	if pragmaSetRes != nil {
		vi.DB.ResultPool().Put(pragmaSetRes)
	}

	if pragmaSetErr != nil {
		slog.Warn("splitOversizedClusters: could not set cache_size", "error", pragmaSetErr)
	}

	defer func() {
		res, err := vi.DB.Exec("PRAGMA cache_size = 0", nil)

		if res != nil {
			vi.DB.ResultPool().Put(res)
		}

		if err != nil {
			slog.Warn("splitOversizedClusters: could not restore cache_size", "error", err)
		}
	}()

	// Keep splitting until no oversized clusters remain
	maxIterations := 10 // Prevent infinite loops

	// Track clusters we've already attempted to split in this call
	// to avoid retry loops on transient failures
	attemptedSplits := make(map[int64]bool)

	for iteration := 0; iteration < maxIterations; iteration++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Find clusters that are too large for this column
		// Use 1.5x MaxClusterSize threshold to avoid excessive splitting
		// This allows some overfill for better tree structure
		splitThreshold := int64(float64(vi.MaxClusterSize) * 1.5)

		var qb strings.Builder
		qb.Grow(96)
		qb.WriteString("SELECT cluster_id, cluster_size FROM ")
		qb.WriteString(vi.TableName)
		qb.WriteString("_")
		qb.WriteString(columnName)
		qb.WriteString("_cluster_tree WHERE cluster_size > ? AND is_leaf = 1")

		res, err := vi.DB.Exec(
			qb.String(),
			[]sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeInteger, Value: splitThreshold},
			},
		)

		if err != nil {
			if res != nil {
				vi.DB.ResultPool().Put(res)
			}

			return err
		}

		// Extract cluster info before returning the result to the pool.
		// Row data becomes invalid after Put (Reset releases Column objects).
		type oversizedCluster struct {
			id   int64
			size int
		}

		oversized := make([]oversizedCluster, len(res.Rows))

		for i, row := range res.Rows {
			oversized[i] = oversizedCluster{row[0].Int64(), int(row[1].Int64())}
		}

		vi.DB.ResultPool().Put(res)

		if len(oversized) == 0 {
			return nil // No more clusters need splitting
		}

		splitSucceeded := false

		for _, oc := range oversized {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			clusterID := oc.id

			// Skip clusters we've already attempted in this call
			if attemptedSplits[clusterID] {
				continue
			}

			attemptedSplits[clusterID] = true

			if err := vi.splitCluster(ctx, columnName, distanceMetric, clusterID); err != nil {
				slog.Error("Failed to split cluster", "column", columnName, "cluster_id", clusterID, "error", err)
				// Continue with other clusters
			} else {
				splitSucceeded = true
			}
		}

		// If we successfully split at least one cluster, allow another iteration
		// Otherwise, exit to avoid infinite loops on permanent failures
		if !splitSucceeded {
			slog.Warn("No clusters successfully split in iteration, stopping",
				"table", vi.TableName,
				"column", columnName,
				"iteration", iteration+1)
			break
		}
	}

	slog.Warn("Cluster splitting reached max iterations",
		"table", vi.TableName,
		"column", columnName,
		"max_iterations", maxIterations)

	return nil
}

// splitCluster splits a single cluster into k child clusters using distance-based quantile split.
//
// The operation is split into three phases to minimize write-lock hold time:
//
//   - Phase 1 (read-only tx): Fetch cluster info and all vectors. Uses BEGIN DEFERRED
//     so concurrent writers (e.g. insert transactions) are not blocked.
//   - Phase 2 (no tx): Compute assignments and centroids in-memory.
//   - Phase 3 (write tx): Apply the split — create children, reassign vectors, update stats.
//     Uses BEGIN IMMEDIATE but holds the write lock only for the short write phase.
//
// This separation allows split reads to overlap with insert transactions, reducing
// end-to-end latency when inserts and splits run concurrently.
//
// Performance notes:
//   - Vector float32 slices are taken from splitVecPool and returned after use.
//   - The INSERT OR REPLACE batch SQL and UPDATE CASE WHEN for cluster sizes are
//     built once per call, not once per inner loop iteration.
//   - encodeFloat32VectorPooled reuses a pooled byte buffer via encodeVecPool.
func (vi *VectorIndexer) splitCluster(ctx context.Context, columnName string, distanceMetric int, clusterID int64) error {
	// B+ tree structure: max 16 children per internal node, ~5000 vectors per leaf
	// Calculate k based on cluster size to target MaxClusterSize vectors per child
	// k = min(16, ceil(cluster_size / MaxClusterSize))
	// For 100k vectors: k = min(16, 100k/5k) = min(16, 20) = 16 -> 6250 per child
	// For 6250 vectors: k = min(16, 6250/5k) = min(16, 2) = 2 -> 3125 per child

	// Get dimensions for this column
	var dimensions int

	for _, col := range vi.VectorColumns {
		if col.Name == columnName {
			dimensions = col.Dimensions
			break
		}
	}

	if dimensions == 0 {
		return fmt.Errorf("column %s not found in vector columns", columnName)
	}

	// Pre-build reusable SQL prefixes used inside the transaction to avoid
	// repeated string allocations.
	treeTablePrefix := vi.TableName + "_" + columnName + "_cluster_tree"
	mapTablePrefix := vi.TableName + "_" + columnName + "_cluster_vector_map"

	infoSQL := "SELECT cluster_size, is_leaf FROM " + treeTablePrefix + " WHERE cluster_id = ?"
	maxIDSQL := "SELECT IFNULL(MAX(cluster_id), 0) FROM " + treeTablePrefix
	insertChildSQL := "INSERT INTO " + treeTablePrefix + " (cluster_id, parent_id, centroid_blob, cluster_size, is_leaf) VALUES (?, ?, ?, 0, 1)"
	setNonLeafSQL := "UPDATE " + treeTablePrefix + " SET is_leaf = 0 WHERE cluster_id = ?"
	zeroSizeSQL := "UPDATE " + treeTablePrefix + " SET cluster_size = 0 WHERE cluster_id = ?"
	fetchVecsSQL := `SELECT m.vector_id, IFNULL(m.distance, 0.0), v.` + columnName + `
		FROM ` + mapTablePrefix + ` m
		JOIN ` + vi.TableName + `_vectors v ON v.rowid = m.vector_id
		WHERE m.cluster_id = ?
		ORDER BY IFNULL(m.distance, 0.0) ASC
		LIMIT 100000`

	// ---- Phase 1: Read-only transaction — fetch cluster info and vectors ----
	// Uses BEGIN DEFERRED so no write lock is held, allowing concurrent inserts
	// to proceed while we read the (potentially large) vector data from WAL.
	type vecEntry struct {
		id       int64
		distance float64
		vector   []float32
	}

	var vectors []vecEntry
	var clusterSize int64
	var isLeaf bool

	readErr := vi.DB.Transaction(true, func(db *DatabaseConnection) error {
		// Read cluster info
		res, err := db.Exec(
			infoSQL,
			[]sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeInteger, Value: clusterID},
			},
		)

		if res != nil {
			defer db.ResultPool().Put(res)
		}

		if err != nil {
			return fmt.Errorf("failed to get cluster info: %w", err)
		}

		if len(res.Rows) == 0 {
			return fmt.Errorf("cluster_id %d not found", clusterID)
		}

		clusterSize = res.Rows[0][0].Int64()
		isLeaf = res.Rows[0][1].Int64() != 0

		// Skip if already split (not a leaf anymore).
		if !isLeaf {
			return nil
		}

		// Fetch vectors for this cluster using ExecStream to avoid buffering all rows.
		// Each vector's float32 slice is allocated from splitVecPool and returned
		// after centroid computation to reduce GC pressure.
		expectedSize := min(int(clusterSize), 100000)
		vectors = make([]vecEntry, 0, expectedSize)

		rowCount := 0

		err = db.ExecStream(
			fetchVecsSQL,
			[]sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeInteger, Value: clusterID},
			},
			func(row []*sqlite3.Column) error {
				if len(row) < 3 {
					return nil
				}

				rowCount++
				vectorID := row[0].Int64()
				distance := row[1].Float64()
				vectorBlob := row[2].Blob()

				// Parse vector using pooled VectorBlob to reduce allocations.
				vb, err := vector.ParseVectorBlobPooled(vectorBlob)

				if err != nil {
					slog.Error("Failed to parse vector blob during split", "id", vectorID, "error", err)
					return nil
				}

				vectorData := vb.GetFloat32Slice()

				if len(vectorData) != dimensions {
					slog.Error("Vector dimension mismatch during split", "id", vectorID, "expected", dimensions, "got", len(vectorData))
					vector.PutVectorBlob(vb)
					return nil
				}

				// Obtain a pooled float32 slice for the copy so each row does
				// not heap-allocate a new backing array.
				dst := getSplitVec(dimensions)
				copy(dst, vectorData)
				vector.PutVectorBlob(vb)

				vectors = append(vectors, vecEntry{
					id:       vectorID,
					distance: distance,
					vector:   dst,
				})

				return nil
			},
		)

		return err
	})

	if readErr != nil {
		return readErr
	}

	// Early exit if not a leaf (already split by another goroutine).
	if !isLeaf {
		return nil
	}

	// ---- Phase 2: In-memory computation — no transaction needed ----
	// Calculate optimal k: enough children to keep each near MaxClusterSize.
	// Don't clamp to 16 — we'll handle >16 children by splitting the internal node.
	k := max(int(math.Ceil(float64(clusterSize)/float64(vi.MaxClusterSize))), 2)

	if len(vectors) < k*2 {
		// Not enough vectors to split meaningfully; return pooled slices.
		for _, v := range vectors {
			putSplitVec(v.vector)
		}

		return nil
	}

	// Split vectors by distance into k equal-sized quantiles (B+ tree style).
	// Vectors are already sorted by distance from closest to furthest.
	vectorsPerChild := len(vectors) / k
	assignments := make([]int, len(vectors))

	for i := range vectors {
		childIdx := i / vectorsPerChild

		// Assign remainder vectors to the last child.
		if childIdx >= k {
			childIdx = k - 1
		}

		assignments[i] = childIdx
	}

	// Compute centroids for each child cluster.
	// Use float64 accumulators to reduce rounding error on large clusters.
	childCentroids := make([][]float32, k)

	for i := range k {
		acc := getSplitVec64(dimensions)
		count := 0

		for j, v := range vectors {
			if assignments[j] == i {
				for dim, val := range v.vector {
					acc[dim] += float64(val)
				}

				count++
			}
		}

		centroid := make([]float32, dimensions)

		if count > 0 {
			invCount := 1.0 / float64(count)

			for dim := range centroid {
				centroid[dim] = float32(acc[dim] * invCount)
			}
		}

		putSplitVec64(acc)
		childCentroids[i] = centroid
	}

	// Return pooled vector slices now that centroid computation is done.
	// We still need id and distance for the write phase.
	for _, v := range vectors {
		putSplitVec(v.vector)
	}

	// ---- Phase 3: Write-only transaction — apply the split ----
	// This is a short write transaction that only performs mutations.
	// The heavy I/O (reading vector blobs) already happened in Phase 1.
	writeErr := vi.DB.Transaction(false, func(db *DatabaseConnection) error {
		// Optimistic verification: ensure the cluster is still a leaf.
		// Another goroutine may have split it between Phase 1 and Phase 3.
		verifyRes, err := db.Exec(
			infoSQL,
			[]sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeInteger, Value: clusterID},
			},
		)

		if err != nil {
			if verifyRes != nil {
				db.ResultPool().Put(verifyRes)
			}

			return fmt.Errorf("failed to verify cluster info: %w", err)
		}

		stillLeaf := len(verifyRes.Rows) > 0 && verifyRes.Rows[0][1].Int64() != 0
		db.ResultPool().Put(verifyRes)

		if !stillLeaf {
			return nil
		}

		// Get the next available cluster_id in one round-trip.
		// Must be inside the write transaction to avoid ID collisions.
		maxIDRes, err := db.Exec(maxIDSQL, nil)

		if err != nil {
			if maxIDRes != nil {
				db.ResultPool().Put(maxIDRes)
			}

			return err
		}

		nextClusterID := maxIDRes.Rows[0][0].Int64() + 1
		db.ResultPool().Put(maxIDRes)

		// Insert k child clusters with their computed centroids.
		for i := range k {
			childClusterID := nextClusterID + int64(i)

			// Encode centroid into a pooled blob buffer.
			centroidBlob := encodeFloat32VectorPooled(childCentroids[i])

			childRes, err := db.Exec(
				insertChildSQL,
				[]sqlite3.StatementParameter{
					{Type: sqlite3.ParameterTypeInteger, Value: childClusterID},
					{Type: sqlite3.ParameterTypeInteger, Value: clusterID},
					{Type: sqlite3.ParameterTypeBlob, Value: centroidBlob},
				},
			)

			if childRes != nil {
				db.ResultPool().Put(childRes)
			}

			putEncodeVec(centroidBlob)

			if err != nil {
				return err
			}
		}

		// Flip the parent from leaf → internal node.
		nonLeafRes, err := db.Exec(
			setNonLeafSQL,
			[]sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeInteger, Value: clusterID},
			},
		)

		if nonLeafRes != nil {
			db.ResultPool().Put(nonLeafRes)
		}

		if err != nil {
			return err
		}

		// Reassign vectors to child clusters in batches.
		// INSERT OR REPLACE updates existing rows in-place (no DELETE+INSERT).
		// SQLite parameter limit: with 3 params per row (vector_id, cluster_id,
		// distance), 10 000 rows = 30 000 params, well below the 32 766 cap.
		const batchSize = 10000

		// Build the fixed-length INSERT prefix once; only the VALUES list changes.
		insertPrefix := "INSERT OR REPLACE INTO " + mapTablePrefix + " (vector_id, cluster_id, distance) VALUES "

		// Per-child size accumulators (indexed by child index 0..k-1) to defer
		// cluster_size updates until after all batches are written.
		childSizeDeltas := make([]int, k)

		// Reuse a parameter buffer from the pool to avoid per-batch allocations.
		paramsBuf := statementParamsPool.Get().([]sqlite3.StatementParameter)

		for start := 0; start < len(vectors); start += batchSize {
			select {
			case <-ctx.Done():
				statementParamsPool.Put(paramsBuf)
				return ctx.Err()
			default:
			}

			end := min(start+batchSize, len(vectors))
			batch := vectors[start:end]
			params := paramsBuf[:len(batch)*3]

			// Build the VALUES list for this batch. Pre-grow to ~12 bytes per row.
			var vbldr strings.Builder
			vbldr.Grow(len(batch) * 12)

			p := 0

			for j, v := range batch {
				childIdx := assignments[start+j]
				childClusterID := nextClusterID + int64(childIdx)

				if j > 0 {
					vbldr.WriteString(", ")
				}

				vbldr.WriteString("(?, ?, ?)")

				params[p] = sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: v.id}
				p++
				params[p] = sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: childClusterID}
				p++
				params[p] = sqlite3.StatementParameter{Type: sqlite3.ParameterTypeFloat, Value: v.distance}
				p++

				childSizeDeltas[childIdx]++
			}

			batchRes, batchErr := db.Exec(insertPrefix+vbldr.String(), params)

			if batchRes != nil {
				db.ResultPool().Put(batchRes)
			}

			if batchErr != nil {
				statementParamsPool.Put(paramsBuf)
				return batchErr
			}
		}

		statementParamsPool.Put(paramsBuf)

		// Batch all child cluster_size increments into a single UPDATE CASE WHEN
		// statement instead of k individual round-trips.
		if err := vi.updateClusterSizesBatch(db, columnName, nextClusterID, childSizeDeltas); err != nil {
			slog.Error("Failed to update child cluster sizes", "cluster", clusterID, "error", err)
		}

		// Zero the parent cluster's size (vectors moved to children).
		zeroRes, err := db.Exec(
			zeroSizeSQL,
			[]sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeInteger, Value: clusterID},
			},
		)

		if zeroRes != nil {
			db.ResultPool().Put(zeroRes)
		}

		if err != nil {
			return err
		}

		// If we created more than 16 children, split the internal node.
		if k > 16 {
			if err = vi.splitInternalNode(db, columnName, clusterID, nextClusterID, k); err != nil {
				return fmt.Errorf("failed to split internal node: %w", err)
			}
		}

		return nil
	})

	return writeErr
}

// splitInternalNode splits an internal node that has too many children (>16)
// into multiple internal nodes, each with at most 16 children
func (vi *VectorIndexer) splitInternalNode(db *DatabaseConnection, columnName string, parentClusterID, firstChildID int64, totalChildren int) error {
	// Get dimensions for this column
	var dimensions int

	for _, col := range vi.VectorColumns {
		if col.Name == columnName {
			dimensions = col.Dimensions
			break
		}
	}

	if dimensions == 0 {
		return fmt.Errorf("column %s not found in vector columns", columnName)
	}

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
		fmt.Sprintf(`SELECT IFNULL(MAX(cluster_id), 0) FROM %s_%s_cluster_tree`, vi.TableName, columnName),
		nil,
	)

	if err != nil {
		if maxIDRes != nil {
			db.ResultPool().Put(maxIDRes)
		}

		return err
	}

	newInternalNodeID := maxIDRes.Rows[0][0].Int64() + 1
	db.ResultPool().Put(maxIDRes)

	// First, compute centroids for each group of children
	// We need this before creating the internal nodes because centroid_blob is NOT NULL
	internalNodeCentroids := make([][]byte, numInternalNodes)

	for i := range numInternalNodes {
		// Calculate which children belong to this internal node
		startIdx := i * maxChildrenPerNode
		endIdx := min((i+1)*maxChildrenPerNode, totalChildren)

		// Get centroids of children that will belong to this internal node
		centroid := make([]float32, dimensions)
		count := 0

		for j := startIdx; j < endIdx; j++ {
			childID := firstChildID + int64(j)

			// Get this child's centroid
			childRes, err := db.Exec(
				fmt.Sprintf(`SELECT centroid_blob FROM %s_%s_cluster_tree WHERE cluster_id = ?`, vi.TableName, columnName),
				[]sqlite3.StatementParameter{
					{Type: sqlite3.ParameterTypeInteger, Value: childID},
				},
			)

			if err != nil || len(childRes.Rows) == 0 {
				if childRes != nil {
					db.ResultPool().Put(childRes)
				}

				continue
			}

			centroidBlob := childRes.Rows[0][0].Blob()

			if len(centroidBlob) == 0 {
				db.ResultPool().Put(childRes)
				continue
			}

			vb, err := vector.ParseVectorBlob(centroidBlob)

			if err != nil {
				db.ResultPool().Put(childRes)
				continue
			}

			childCentroid := vb.GetFloat32Slice()

			// Done with childRes — return to pool before processing.
			db.ResultPool().Put(childRes)

			if len(childCentroid) != dimensions {
				continue
			}

			for dim := 0; dim < dimensions; dim++ {
				centroid[dim] += childCentroid[dim]
			}

			count++
		}

		// Compute mean
		if count > 0 {
			for dim := 0; dim < dimensions; dim++ {
				centroid[dim] /= float32(count)
			}
		}

		internalNodeCentroids[i] = encodeFloat32Vector(centroid)
	}

	// Create new internal nodes with computed centroids (one less than numInternalNodes, since the original parent becomes one)
	for i := 1; i < numInternalNodes; i++ {
		insRes, err := db.Exec(
			fmt.Sprintf(`INSERT INTO %s_%s_cluster_tree (cluster_id, parent_id, centroid_blob, cluster_size, is_leaf) VALUES (?, ?, ?, 0, 0)`, vi.TableName, columnName),
			[]sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeInteger, Value: newInternalNodeID + int64(i) - 1},
				{Type: sqlite3.ParameterTypeInteger, Value: parentClusterID},
				{Type: sqlite3.ParameterTypeBlob, Value: internalNodeCentroids[i]},
			},
		)

		if insRes != nil {
			db.ResultPool().Put(insRes)
		}

		if err != nil {
			return err
		}
	}

	// Update the original parent's centroid
	var ucb strings.Builder
	ucb.Grow(len(vi.TableName) + len(columnName) + 80)
	ucb.WriteString("UPDATE ")
	ucb.WriteString(vi.TableName)
	ucb.WriteString("_")
	ucb.WriteString(columnName)
	ucb.WriteString("_cluster_tree SET centroid_blob = ? WHERE cluster_id = ?")

	ucbRes, err := db.Exec(
		ucb.String(),
		[]sqlite3.StatementParameter{
			{Type: sqlite3.ParameterTypeBlob, Value: internalNodeCentroids[0]},
			{Type: sqlite3.ParameterTypeInteger, Value: parentClusterID},
		},
	)

	if ucbRes != nil {
		db.ResultPool().Put(ucbRes)
	}

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

		reassignRes, err := db.Exec(
			fmt.Sprintf(`UPDATE %s_%s_cluster_tree SET parent_id = ? WHERE cluster_id = ?`, vi.TableName, columnName),
			[]sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeInteger, Value: newParentID},
				{Type: sqlite3.ParameterTypeInteger, Value: childID},
			},
		)

		if reassignRes != nil {
			db.ResultPool().Put(reassignRes)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

// encodeFloat32Vector encodes a float32 vector into the VectorBlob format.
// The returned slice is always freshly allocated; callers that can tolerate
// a pooled buffer should use encodeFloat32VectorPooled + putEncodeVec instead.
func encodeFloat32Vector(vec []float32) []byte {
	const (
		vectorVersion1 = 0x01
		vectorTypeF32  = 0x01
	)

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

// encodeVecPool holds pooled []byte buffers for centroid encoding inside
// splitCluster. Buffers are sized to the exact blob length on Get and must be
// returned via putEncodeVec when the caller no longer needs them.
var encodeVecPool = sync.Pool{
	New: func() interface{} { b := make([]byte, 0, 6+512*4); return &b },
}

// encodeFloat32VectorPooled writes Vec into a pooled byte buffer and returns
// it. The caller must call putEncodeVec(blob) once the buffer is no longer
// referenced (e.g. after the SQLite bind call copies the bytes).
func encodeFloat32VectorPooled(vec []float32) []byte {
	blobSize := 6 + len(vec)*4
	bp := encodeVecPool.Get().(*[]byte)
	blob := (*bp)[:0]

	if cap(blob) < blobSize {
		blob = make([]byte, blobSize)
	} else {
		blob = blob[:blobSize]
	}

	blob[0] = 0x01 // version
	blob[1] = 0x01 // float32 type

	dims := uint32(len(vec))
	blob[2] = byte(dims)
	blob[3] = byte(dims >> 8)
	blob[4] = byte(dims >> 16)
	blob[5] = byte(dims >> 24)

	offset := 6

	for _, val := range vec {
		bits := math.Float32bits(val)
		blob[offset] = byte(bits)
		blob[offset+1] = byte(bits >> 8)
		blob[offset+2] = byte(bits >> 16)
		blob[offset+3] = byte(bits >> 24)
		offset += 4
	}

	// Store the slice header back so the pool object stays consistent.
	*bp = blob

	return blob
}

// putEncodeVec returns a pooled encode buffer obtained from encodeFloat32VectorPooled.
func putEncodeVec(b []byte) {
	bp := &b
	*bp = b[:0]
	encodeVecPool.Put(bp)
}

// splitVecPool holds pooled []float32 buffers used for per-vector copies
// during cluster splitting. Dimensions vary per table so buffers may be
// resized; they are always returned with their original capacity.
var splitVecPool = sync.Pool{
	New: func() interface{} { s := make([]float32, 0, 512); return &s },
}

// getSplitVec returns a pooled float32 slice sized to dims.
func getSplitVec(dims int) []float32 {
	sp := splitVecPool.Get().(*[]float32)
	s := *sp

	if cap(s) < dims {
		s = make([]float32, dims)
	} else {
		s = s[:dims]
	}

	*sp = s

	return s
}

// putSplitVec returns a pooled float32 slice to the pool.
func putSplitVec(s []float32) {
	sp := splitVecPool.Get().(*[]float32)
	*sp = s[:0]
	splitVecPool.Put(sp)
}

// splitVec64Pool holds pooled []float64 accumulators for centroid computation.
var splitVec64Pool = sync.Pool{
	New: func() interface{} { s := make([]float64, 0, 512); return &s },
}

// getSplitVec64 returns a zeroed pooled float64 accumulator slice of length dims.
func getSplitVec64(dims int) []float64 {
	sp := splitVec64Pool.Get().(*[]float64)
	s := *sp

	if cap(s) < dims {
		s = make([]float64, dims)
	} else {
		s = s[:dims]
		// Zero the slice for reuse.
		for i := range s {
			s[i] = 0
		}
	}

	*sp = s

	return s
}

// putSplitVec64 returns a pooled float64 accumulator to the pool.
func putSplitVec64(s []float64) {
	sp := splitVec64Pool.Get().(*[]float64)
	*sp = s[:0]
	splitVec64Pool.Put(sp)
}

// updateClusterSizesBatch increments the cluster_size of k consecutive child
// clusters (IDs nextClusterID … nextClusterID+k-1) using a single
// UPDATE … SET cluster_size = CASE WHEN … END statement, replacing k
// individual round-trips with one.
func (vi *VectorIndexer) updateClusterSizesBatch(
	db *DatabaseConnection,
	columnName string,
	nextClusterID int64,
	deltas []int, // deltas[i] is the size increment for cluster nextClusterID+i
) error {
	// Count non-zero deltas to skip empty children.
	activeCount := 0

	for _, d := range deltas {
		if d > 0 {
			activeCount++
		}
	}

	if activeCount == 0 {
		return nil
	}

	treeTable := vi.TableName + "_" + columnName + "_cluster_tree"

	// Build: UPDATE t SET cluster_size = CASE cluster_id
	//          WHEN id1 THEN cluster_size+d1
	//          WHEN id2 THEN cluster_size+d2
	//          …
	//        END WHERE cluster_id IN (id1, id2, …)
	//
	// All values are inlined as integer literals to avoid parameter-count
	// pressure and keep the statement short enough for SQLite's prepared-
	// statement cache.
	var sb strings.Builder
	sb.Grow(64 + activeCount*48)
	sb.WriteString("UPDATE ")
	sb.WriteString(treeTable)
	sb.WriteString(" SET cluster_size = CASE cluster_id")

	for i, d := range deltas {
		if d <= 0 {
			continue
		}

		childID := nextClusterID + int64(i)
		sb.WriteString(" WHEN ")
		sb.WriteString(fmt.Sprintf("%d", childID))
		sb.WriteString(" THEN cluster_size+")
		sb.WriteString(fmt.Sprintf("%d", d))
	}

	sb.WriteString(" END WHERE cluster_id IN (")

	first := true

	for i, d := range deltas {
		if d <= 0 {
			continue
		}

		if !first {
			sb.WriteString(",")
		}

		sb.WriteString(fmt.Sprintf("%d", nextClusterID+int64(i)))
		first = false
	}

	sb.WriteString(")")

	res, err := db.Exec(sb.String(), nil)

	if res != nil {
		db.ResultPool().Put(res)
	}

	return err
}
