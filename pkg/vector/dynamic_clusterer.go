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

// Dynamic clustering parameters
const (
	DefaultMaxClusterSize = 5000
	DefaultMinClusterSize = 200
)

// ClusterInfo represents a cluster's metadata and centroid
type ClusterInfo struct {
	ClusterID      int64
	CentroidBlob   []byte
	Size           int
	Version        int64
	CreatedAt      time.Time
	UpdatedAt      time.Time
	Centroid       []float32 // Decoded centroid
	DistanceMetric int
}

// DynamicClusterer manages dynamic clustering operations with automatic splitting
type DynamicClusterer struct {
	DB             *database.DatabaseConnection
	TableName      string
	Dimensions     int
	DistanceMetric int
	MaxClusterSize int
	MinClusterSize int
	distanceFunc   DistanceFunc
}

// NewDynamicClusterer creates a new dynamic clusterer
func NewDynamicClusterer(db *database.DatabaseConnection, tableName string, dimensions, distanceMetric, maxClusterSize, minClusterSize int) (*DynamicClusterer, error) {
	clusterer := &DynamicClusterer{
		DB:             db,
		TableName:      tableName,
		Dimensions:     dimensions,
		DistanceMetric: distanceMetric,
		MaxClusterSize: maxClusterSize,
		MinClusterSize: minClusterSize,
	}

	// Set distance function based on metric
	clusterer.distanceFunc = GetDistanceFunc(distanceMetric)

	return clusterer, nil
}

// AssignToCluster assigns a vector to the best cluster
func (c *DynamicClusterer) AssignToCluster(vector []float32) (int64, error) {
	clusters, err := c.GetAllClusters()

	if err != nil {
		return 0, fmt.Errorf("failed to get clusters: %w", err)
	}

	// If no clusters exist, create the first one
	if len(clusters) == 0 {
		return c.CreateCluster(vector)
	}

	// Find nearest cluster
	var bestClusterID int64
	bestDistance := float32(math.Inf(1))

	for _, cluster := range clusters {
		if cluster.Size >= c.MaxClusterSize {
			continue // Skip full clusters
		}

		distance := c.distanceFunc(vector, cluster.Centroid)

		if distance < bestDistance {
			bestDistance = distance
			bestClusterID = cluster.ClusterID
		}
	}

	// If all clusters are full, create a new one
	if bestClusterID == 0 {
		return c.CreateCluster(vector)
	}

	return bestClusterID, nil
}

// GetAllClusters retrieves all clusters with their centroids
func (c *DynamicClusterer) GetAllClusters() ([]*ClusterInfo, error) {
	res, err := c.DB.Exec(
		fmt.Sprintf(`SELECT cluster_id, centroid_blob, cluster_size, version 
		FROM %s_clusters ORDER BY cluster_id`, c.TableName),
		nil,
	)

	if err != nil {
		return nil, err
	}

	var clusters []*ClusterInfo

	for _, row := range res.Rows {
		if len(row) < 4 {
			continue
		}

		clusterID := row[0].Int64()
		centroidBlob := row[1].Blob()
		size := int(row[2].Int64())
		version := row[3].Int64()

		// Parse centroid blob properly
		centroidVec, err := ParseVectorBlob(centroidBlob)
		if err != nil {
			slog.Warn("Failed to parse centroid blob", "cluster_id", clusterID, "error", err)
			continue
		}

		cluster := &ClusterInfo{
			ClusterID:      clusterID,
			CentroidBlob:   centroidBlob,
			Size:           size,
			Version:        version,
			Centroid:       centroidVec.GetFloat32Slice(),
			DistanceMetric: c.DistanceMetric,
		}

		clusters = append(clusters, cluster)
	}

	return clusters, nil
}

// CreateCluster creates a new cluster with the given centroid
func (c *DynamicClusterer) CreateCluster(centroid []float32) (int64, error) {
	centroidBlob, err := EncodeFloat32(centroid)

	if err != nil {
		return 0, fmt.Errorf("failed to encode centroid: %w", err)
	}

	_, err = c.DB.Exec(
		fmt.Sprintf(`INSERT INTO %s_clusters (centroid_blob, cluster_size, version) 
		VALUES (?, 0, 1)`, c.TableName),
		[]sqlite3.StatementParameter{
			{Type: "BLOB", Value: centroidBlob},
		},
	)

	if err != nil {
		return 0, err
	}

	// Get the last inserted cluster ID
	res, err := c.DB.Exec(
		fmt.Sprintf(`SELECT cluster_id FROM %s_clusters ORDER BY cluster_id DESC LIMIT 1`, c.TableName),
		nil,
	)

	if err != nil || len(res.Rows) == 0 {
		return 0, fmt.Errorf("failed to get new cluster ID")
	}

	clusterID := res.Rows[0][0].Int64()

	return clusterID, nil
}

// UpdateCentroid updates a cluster's centroid incrementally
func (c *DynamicClusterer) UpdateCentroid(clusterID int64, vector []float32, operation string) error {
	// Get current cluster info
	res, err := c.DB.Exec(
		fmt.Sprintf(`SELECT centroid_blob, cluster_size FROM %s_clusters WHERE cluster_id = ?`, c.TableName),
		[]sqlite3.StatementParameter{
			{Type: sqlite3.ParameterTypeInteger, Value: clusterID},
		},
	)

	if err != nil || len(res.Rows) == 0 {
		return fmt.Errorf("cluster not found")
	}

	centroidBlob := res.Rows[0][0].Blob()
	size := int(res.Rows[0][1].Int64())

	// Parse current centroid
	centroidVec, err := ParseVectorBlob(centroidBlob)
	if err != nil {
		return fmt.Errorf("failed to parse centroid blob: %w", err)
	}
	centroid := centroidVec.GetFloat32Slice()

	// Update centroid in-place and determine size delta
	var sizeDelta int64

	switch operation {
	case "INSERT":
		// Incremental update: C_new = (C_old * n + v) / (n + 1)
		divisor := float32(size + 1)

		for i := range centroid {
			centroid[i] = (centroid[i]*float32(size) + vector[i]) / divisor
		}

		sizeDelta = 1
	case "DELETE":
		// Incremental removal: C_new = (C_old * n - v) / (n - 1)
		if size <= 1 {
			// If this was the last vector, reset to zero centroid
			for i := range centroid {
				centroid[i] = 0
			}
		} else {
			divisor := float32(size - 1)

			for i := range centroid {
				centroid[i] = (centroid[i]*float32(size) - vector[i]) / divisor
			}
		}

		sizeDelta = -1
	default:
		return fmt.Errorf("invalid operation: %s", operation)
	}

	newCentroidBlob, err := EncodeFloat32(centroid)

	if err != nil {
		return fmt.Errorf("failed to encode new centroid: %w", err)
	}

	// Update centroid and size in a single query
	_, err = c.DB.Exec(
		fmt.Sprintf(`UPDATE %s_clusters SET centroid_blob = ?, cluster_size = cluster_size + ?, version = version + 1 
		WHERE cluster_id = ?`, c.TableName),
		[]sqlite3.StatementParameter{
			{Type: sqlite3.ParameterTypeBlob, Value: newCentroidBlob},
			{Type: sqlite3.ParameterTypeInteger, Value: sizeDelta},
			{Type: sqlite3.ParameterTypeInteger, Value: clusterID},
		},
	)

	return err
}

// CheckAndRebalance checks if any clusters need rebalancing
func (c *DynamicClusterer) CheckAndRebalance() error {
	clusters, err := c.GetAllClusters()

	if err != nil {
		return err
	}

	for _, cluster := range clusters {
		if cluster.Size > c.MaxClusterSize {
			if err := c.SplitCluster(cluster.ClusterID); err != nil {
				slog.Error("Failed to split cluster", "cluster_id", cluster.ClusterID, "error", err)
			}
		}
	}

	return nil
}

// CheckAndRebalanceClusters checks if specific clusters need rebalancing
// This is more efficient than CheckAndRebalance when you know which clusters were modified
func (c *DynamicClusterer) CheckAndRebalanceClusters(ctx context.Context, clusterIDs []int64) error {
	if len(clusterIDs) == 0 {
		return nil
	}

	// Get cluster sizes for the specified clusters
	placeholders := make([]string, len(clusterIDs))
	params := make([]sqlite3.StatementParameter, len(clusterIDs))

	for i, id := range clusterIDs {
		placeholders[i] = "?"
		params[i] = sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: id}
	}

	query := fmt.Sprintf(`SELECT cluster_id, cluster_size FROM %s_clusters WHERE cluster_id IN (%s)`,
		c.TableName, strings.Join(placeholders, ","))

	res, err := c.DB.Exec(query, params)

	if err != nil {
		return err
	}

	// Check each cluster and split if needed
	for _, row := range res.Rows {
		if len(row) < 2 {
			continue
		}

		clusterID := row[0].Int64()
		size := int(row[1].Int64())

		if size > c.MaxClusterSize {
			// Check context before expensive split operation
			select {
			case <-ctx.Done():
				slog.Debug("Skipping cluster split due to shutdown", "cluster_id", clusterID)
				return ctx.Err()
			default:
				if err := c.SplitCluster(clusterID); err != nil {
					slog.Error("Failed to split cluster", "cluster_id", clusterID, "error", err)
				}
			}
		}
	}

	return nil
}

// SplitCluster splits an oversized cluster into two clusters
func (c *DynamicClusterer) SplitCluster(clusterID int64) error {
	// Get the vector column name from metadata
	vectorColumns, err := getVectorColumns(c.DB, c.TableName)

	if err != nil {
		return fmt.Errorf("failed to get vector columns: %w", err)
	}

	if len(vectorColumns) == 0 {
		return fmt.Errorf("no vector columns found")
	}

	// Use first vector column (dynamic clusterer is deprecated)
	vectorColumn := vectorColumns[0].Name

	// Get all vectors from the cluster
	res, err := c.DB.Exec(
		fmt.Sprintf(`SELECT id, %s FROM %s_indexed WHERE cluster_id = ?`, vectorColumn, c.TableName),
		[]sqlite3.StatementParameter{
			{Type: sqlite3.ParameterTypeInteger, Value: clusterID},
		},
	)

	if err != nil {
		return err
	}

	if len(res.Rows) <= 1 {
		return nil // Nothing to split
	}

	// Use k-means++ initialization to find two initial centroids
	vectors := make([][]float32, 0, len(res.Rows))
	vectorIDs := make([]int64, 0, len(res.Rows))

	for _, row := range res.Rows {
		if len(row) < 2 {
			continue
		}

		id := row[0].Int64()
		vectorBlob := row[1].Blob()

		// Parse vector blob properly
		vec, err := ParseVectorBlob(vectorBlob)
		if err != nil {
			slog.Warn("Failed to parse vector blob in split", "vector_id", id, "error", err)
			continue
		}
		vector := vec.GetFloat32Slice()

		vectors = append(vectors, vector)
		vectorIDs = append(vectorIDs, id)
	}

	if len(vectors) <= 1 {
		return nil
	}

	// Split vectors using simple k-means (k=2)
	centroid1 := vectors[0]
	centroid2 := vectors[len(vectors)/2]

	// Create new cluster for split
	newClusterID, err := c.CreateCluster(centroid2)

	if err != nil {
		return err
	}

	// Reassign vectors to nearest cluster
	// Batch UPDATEs to avoid overwhelming CGO boundary with thousands of individual calls
	const updateBatchSize = 500
	var clusterIDUpdates = make([]int64, 0, updateBatchSize)
	var newClusterIDUpdates = make([]int64, 0, updateBatchSize)

	for i, vector := range vectors {
		dist1 := c.distanceFunc(vector, centroid1)
		dist2 := c.distanceFunc(vector, centroid2)

		if dist1 < dist2 {
			clusterIDUpdates = append(clusterIDUpdates, vectorIDs[i])
		} else {
			newClusterIDUpdates = append(newClusterIDUpdates, vectorIDs[i])
		}
	}

	// Batch update vectors staying in original cluster
	for i := 0; i < len(clusterIDUpdates); i += updateBatchSize {
		end := min(i+updateBatchSize, len(clusterIDUpdates))

		batch := clusterIDUpdates[i:end]

		if len(batch) == 0 {
			continue
		}

		// Build IN clause with placeholders
		placeholders := make([]string, len(batch))

		for j := range placeholders {
			placeholders[j] = "?"
		}

		inClause := strings.Join(placeholders, ", ")
		params := make([]sqlite3.StatementParameter, len(batch)+1)
		params[0] = sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: clusterID}

		for j, id := range batch {
			params[j+1] = sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: id}
		}

		_, err := c.DB.Exec(
			fmt.Sprintf(`UPDATE %s_indexed SET cluster_id = ?, cluster_version = cluster_version + 1 WHERE id IN (%s)`, c.TableName, inClause),
			params,
		)

		if err != nil {
			// Ignore interrupt errors during shutdown - split will be retried later
			if strings.Contains(strings.ToLower(err.Error()), "interrupt") {
				slog.Debug("Cluster split interrupted during shutdown, will retry later", "cluster_id", clusterID)

				return nil // Treat interrupt as non-fatal
			}

			slog.Error("Failed to reassign vectors to original cluster during split", "batch_size", len(batch), "error", err)
			return err
		}
	}

	// Batch update vectors moving to new cluster
	for i := 0; i < len(newClusterIDUpdates); i += updateBatchSize {
		end := i + updateBatchSize

		if end > len(newClusterIDUpdates) {
			end = len(newClusterIDUpdates)
		}

		batch := newClusterIDUpdates[i:end]

		if len(batch) == 0 {
			continue
		}

		// Build IN clause with placeholders
		placeholders := make([]string, len(batch))

		for j := range placeholders {
			placeholders[j] = "?"
		}

		inClause := strings.Join(placeholders, ", ")
		params := make([]sqlite3.StatementParameter, len(batch)+1)
		params[0] = sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: newClusterID}

		for j, id := range batch {
			params[j+1] = sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: id}
		}

		_, err := c.DB.Exec(
			fmt.Sprintf(`UPDATE %s_indexed SET cluster_id = ?, cluster_version = cluster_version + 1 WHERE id IN (%s)`, c.TableName, inClause),
			params,
		)

		if err != nil {
			// Ignore interrupt errors during shutdown - split will be retried later
			if strings.Contains(strings.ToLower(err.Error()), "interrupt") {
				slog.Debug("Cluster split interrupted during shutdown, will retry later", "cluster_id", newClusterID)

				return nil // Treat interrupt as non-fatal
			}

			slog.Error("Failed to reassign vectors to new cluster during split", "batch_size", len(batch), "error", err)
			return err
		}
	}

	// Update cluster sizes
	res, err = c.DB.Exec(
		fmt.Sprintf(`SELECT COUNT(*) FROM %s_indexed WHERE cluster_id = ?`, c.TableName),
		[]sqlite3.StatementParameter{
			{Type: sqlite3.ParameterTypeInteger, Value: clusterID},
		},
	)

	if err == nil && len(res.Rows) > 0 {
		count := int(res.Rows[0][0].Int64())

		_, _ = c.DB.Exec(
			fmt.Sprintf(`UPDATE %s_clusters SET cluster_size = ? WHERE cluster_id = ?`, c.TableName),
			[]sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeInteger, Value: int64(count)},
				{Type: sqlite3.ParameterTypeInteger, Value: clusterID},
			},
		)
	}

	res, err = c.DB.Exec(
		fmt.Sprintf(`SELECT COUNT(*) FROM %s_indexed WHERE cluster_id = ?`, c.TableName),
		[]sqlite3.StatementParameter{
			{Type: sqlite3.ParameterTypeInteger, Value: newClusterID},
		},
	)

	if err == nil && len(res.Rows) > 0 {
		count := int(res.Rows[0][0].Int64())

		_, _ = c.DB.Exec(
			fmt.Sprintf(`UPDATE %s_clusters SET cluster_size = ? WHERE cluster_id = ?`, c.TableName),
			[]sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeInteger, Value: int64(count)},
				{Type: sqlite3.ParameterTypeInteger, Value: newClusterID},
			},
		)
	}

	return nil
}
