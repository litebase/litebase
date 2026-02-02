package vector

/*
#include <stdlib.h>
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"unsafe"

	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/sqlite3"
)

var (
	searchResultsMap   = make(map[int64]*SearchHandle)
	searchResultsMutex sync.Mutex
	nextSearchID       int64 = 1
)

// SearchHandle holds the results of a vector search
type SearchHandle struct {
	Results []VectorResult
	Index   int
}

// VectorSearch performs a k-NN vector search using pre-built cluster index
// tableName should be the name of a vector_index virtual table (not a regular table)
func VectorSearch(vfsID, databaseID, branchID, tableName, columnName string, queryBlob []byte, k int) (int64, error) {
	// Parse query vector
	queryVector, err := ParseVectorBlob(queryBlob)

	if err != nil {
		return -1, fmt.Errorf("failed to parse query vector: %w", err)
	}

	// Execute cluster-based search using the index's shadow tables
	// The metric is read from the table's metadata
	results, err := executeClusterSearch(vfsID, databaseID, branchID, tableName, queryVector, k)

	if err != nil {
		return -1, err
	}

	// Create handle
	handle := &SearchHandle{
		Results: results,
		Index:   0,
	}

	// Store in map and return ID
	searchResultsMutex.Lock()
	defer searchResultsMutex.Unlock()

	handleID := nextSearchID
	nextSearchID++
	searchResultsMap[handleID] = handle

	return handleID, nil
}

// GetSearchResult retrieves the next result from a search handle
func GetSearchResult(handleID int64) (rowid int64, distance float64, hasMore bool) {
	searchResultsMutex.Lock()
	defer searchResultsMutex.Unlock()

	handle, exists := searchResultsMap[handleID]

	if !exists || handle.Index >= len(handle.Results) {
		return 0, 0, false
	}

	result := handle.Results[handle.Index]
	handle.Index++

	// Return hasMore=true even for the last element since we're returning valid data
	// The next call will return hasMore=false when Index >= len(Results)
	return result.RowId, result.Distance, true
}

// ReleaseSearchResults releases a search handle
func ReleaseSearchResults(handleID int64) {
	searchResultsMutex.Lock()
	defer searchResultsMutex.Unlock()

	delete(searchResultsMap, handleID)
}

// ClusterDistance represents a cluster and its distance from query
type ClusterDistance struct {
	ClusterID int64
	Distance  float64
}

// GetSearchResultsJSON returns all search results as JSON
func GetSearchResultsJSON(handleID int64) (string, error) {
	searchResultsMutex.Lock()
	defer searchResultsMutex.Unlock()

	handle, exists := searchResultsMap[handleID]

	if !exists {
		return "", fmt.Errorf("search handle %d not found", handleID)
	}

	// Convert results to JSON-friendly format
	type ResultJSON struct {
		RowID    int64   `json:"id"`
		Distance float64 `json:"distance"`
	}

	results := make([]ResultJSON, len(handle.Results))

	for i, r := range handle.Results {
		results[i] = ResultJSON{
			RowID:    r.RowId,
			Distance: r.Distance,
		}
	}

	jsonBytes, err := json.Marshal(results)

	if err != nil {
		return "", err
	}

	return string(jsonBytes), nil
}

// goVectorSearch is the CGO export for vector_search()
//
//export goVectorSearch
func goVectorSearch(
	vfsIDPtr *C.char,
	dbIDPtr *C.char,
	branchIDPtr *C.char,
	tableName *C.char,
	columnName *C.char,
	queryBlob unsafe.Pointer,
	queryBlobLen C.int,
	k C.int,
) C.longlong {
	vfsID := C.GoString(vfsIDPtr)
	databaseID := C.GoString(dbIDPtr)
	branchID := C.GoString(branchIDPtr)
	table := C.GoString(tableName)
	column := C.GoString(columnName)

	// Convert query blob to Go byte slice
	queryData := C.GoBytes(queryBlob, queryBlobLen)

	handleID, err := VectorSearch(vfsID, databaseID, branchID, table, column, queryData, int(k))

	if err != nil {
		slog.Error("goVectorSearch failed", "error", err)
		return C.longlong(-1)
	}

	return C.longlong(handleID)
}

//export goGetSearchResult
func goGetSearchResult(handleID C.longlong, rowid *C.longlong, distance *C.double) C.int {
	rid, dist, hasMore := GetSearchResult(int64(handleID))

	if !hasMore {
		return 0
	}

	*rowid = C.longlong(rid)
	*distance = C.double(dist)

	return 1
}

//export goReleaseSearchResults
func goReleaseSearchResults(handleID C.longlong) {
	ReleaseSearchResults(int64(handleID))
}

// executeClusterSearch performs k-NN search using pre-built cluster index
// This is the fast path that queries shadow tables instead of scanning raw data
func executeClusterSearch(vfsID, databaseID, branchID, indexTableName string, queryVector *VectorBlob, k int) ([]VectorResult, error) {
	// Get connection to query the index shadow tables
	conn, err := AcquireConnection(vfsID, databaseID, branchID)

	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}

	defer ReleaseConnection(conn)

	dbConn := conn.GetConnection()

	// Use the connection's result pool
	resultPool := dbConn.ResultPool()

	// Read distance metric from table metadata
	metricQuery := fmt.Sprintf(`SELECT value FROM %s_metadata WHERE key = 'distance_metric'`, indexTableName)

	metricResult, err := dbConn.Exec(metricQuery, nil)

	if err != nil {
		return nil, fmt.Errorf("failed to query distance metric from metadata: %w", err)
	}

	defer resultPool.Put(metricResult)

	if len(metricResult.Rows) == 0 {
		return nil, fmt.Errorf("distance metric not found in metadata for table %s", indexTableName)
	}

	// Convert distance_metric from string to metric name
	metricValue := string(metricResult.Rows[0][0].Text())

	var metric string

	switch metricValue {
	case "0":
		metric = "L2"
	case "1":
		metric = "cosine"
	case "2":
		metric = "dot"
	default:
		metric = "L2" // Default to L2 if unknown
	}

	// Step 1: Find nearest cluster centroids
	// Aggressive optimization: for typical k values (<=20), search only 1 cluster
	// For very large k, scale up but cap at 3 clusters
	numClustersToSearch := 1

	if k > 20 {
		numClustersToSearch = min(2, max(1, k/20))
	}

	if k > 50 {
		numClustersToSearch = 3
	}

	clustersQuery := fmt.Sprintf(`
		SELECT cluster_id, centroid_blob 
		FROM %s_clusters 
		WHERE is_active = 1
	`, indexTableName)

	// Use Exec for query execution (reads don't need barriers - versioning provides safety)
	clustersResult, err := dbConn.Exec(clustersQuery, nil)

	if err != nil {
		return nil, fmt.Errorf("failed to query clusters: %w", err)
	}

	defer resultPool.Put(clustersResult)

	slog.Debug("Cluster query result", "rows", len(clustersResult.Rows), "table", indexTableName)

	if len(clustersResult.Rows) == 0 {
		// No clusters exist yet - fall back to brute-force search on pending vectors
		slog.Debug("No clusters found, performing brute-force search on pending vectors", "table", indexTableName)
		return executeBruteForceSearch(dbConn, resultPool, indexTableName, queryVector, k, metric)
	}

	// Calculate distances to all cluster centroids
	clusterDistances := make([]ClusterDistance, 0, len(clustersResult.Rows))

	for _, row := range clustersResult.Rows {
		clusterID := row[0].Int64()
		centroidBlob := row[1].ColumnValue

		centroid, err := ParseVectorBlob(centroidBlob)

		if err != nil {
			slog.Warn("Failed to parse centroid blob", "cluster_id", clusterID, "error", err)
			continue
		}

		// Calculate distance based on metric
		var dist float64

		switch metric {
		case "L2", "l2":
			dist, err = DistanceL2(queryVector, centroid)
		case "cosine":
			dist, err = DistanceCosine(queryVector, centroid)
		case "dot":
			dist, err = DistanceDot(queryVector, centroid)
		default:
			dist, err = DistanceL2(queryVector, centroid)
		}

		if err != nil {
			slog.Warn("Failed to calculate distance to centroid", "cluster_id", clusterID, "error", err)
			continue
		}

		clusterDistances = append(clusterDistances, ClusterDistance{
			ClusterID: clusterID,
			Distance:  dist,
		})
	}

	// Sort clusters by distance and select top N
	sortClustersByDistance(clusterDistances)

	searchClusters := min(numClustersToSearch, len(clusterDistances))
	topClusters := clusterDistances[:searchClusters]

	// Step 2: Search within selected clusters in parallel using goroutines
	// Each cluster is processed concurrently with its own connection
	resultHeap := NewTopKHeap(k)

	if len(topClusters) > 0 {
		// Create channels for parallel processing
		type clusterResult struct {
			vectors []VectorResult
			err     error
		}

		resultsChan := make(chan clusterResult, len(topClusters)+1) // +1 for pending vectors
		var wg sync.WaitGroup

		// Query each cluster in parallel
		for _, cluster := range topClusters {
			wg.Add(1)

			go func(clusterID int64) {
				defer wg.Done()

				// Get dedicated connection for this cluster
				conn, err := AcquireConnection(vfsID, databaseID, branchID)

				if err != nil {
					resultsChan <- clusterResult{err: err}
					return
				}

				defer ReleaseConnection(conn)

				clusterConn := conn.GetConnection()
				clusterPool := clusterConn.ResultPool()

				// Query vectors in this cluster
				vectorsQuery := fmt.Sprintf(`
					SELECT id, vector_blob 
					FROM %s_indexed 
					WHERE cluster_id = ?
				`, indexTableName)

				vectorsResult, err := clusterConn.Exec(vectorsQuery, []sqlite3.StatementParameter{
					{Type: sqlite3.ParameterTypeInteger, Value: clusterID},
				})

				if err != nil {
					resultsChan <- clusterResult{err: err}
					return
				}

				defer clusterPool.Put(vectorsResult)

				// Calculate distances for all vectors in this cluster
				clusterVectors := make([]VectorResult, 0, len(vectorsResult.Rows))

				for _, row := range vectorsResult.Rows {
					vectorID := row[0].Int64()
					vectorBlob := row[1].ColumnValue

					vector, err := ParseVectorBlob(vectorBlob)

					if err != nil {
						continue
					}

					var dist float64

					switch metric {
					case "L2", "l2":
						dist, _ = DistanceL2(queryVector, vector)
					case "cosine":
						dist, _ = DistanceCosine(queryVector, vector)
					case "dot":
						dist, _ = DistanceDot(queryVector, vector)
					default:
						dist, _ = DistanceL2(queryVector, vector)
					}

					clusterVectors = append(clusterVectors, VectorResult{
						RowId:    vectorID,
						Distance: dist,
					})
				}

				resultsChan <- clusterResult{vectors: clusterVectors}
			}(cluster.ClusterID)
		}

		// Also query pending vectors in parallel
		wg.Go(func() {
			conn, err := AcquireConnection(vfsID, databaseID, branchID)

			if err != nil {
				resultsChan <- clusterResult{err: err}
				return
			}

			defer ReleaseConnection(conn)

			pendingConn := conn.GetConnection()
			pendingPool := pendingConn.ResultPool()

			// Check if there are pending vectors
			pendingCountQuery := fmt.Sprintf(`
				SELECT COUNT(*) 
				FROM %s_pending
				WHERE operation = 'INSERT' AND vector_blob IS NOT NULL
			`, indexTableName)

			pendingCountResult, err := pendingConn.Exec(pendingCountQuery, nil)

			if err != nil {
				resultsChan <- clusterResult{err: err}
				return
			}

			defer pendingPool.Put(pendingCountResult)

			if len(pendingCountResult.Rows) == 0 || pendingCountResult.Rows[0][0].Int64() == 0 {
				resultsChan <- clusterResult{vectors: []VectorResult{}}
				return
			}

			// Query pending vectors
			pendingQuery := fmt.Sprintf(`
				SELECT id, vector_blob 
				FROM %s_pending
				WHERE operation = 'INSERT' AND vector_blob IS NOT NULL
			`, indexTableName)

			pendingResult, err := pendingConn.Exec(pendingQuery, nil)

			if err != nil {
				resultsChan <- clusterResult{err: err}
				return
			}

			defer pendingPool.Put(pendingResult)

			pendingVectors := make([]VectorResult, 0, len(pendingResult.Rows))

			for _, row := range pendingResult.Rows {
				vectorID := row[0].Int64()
				vectorBlob := row[1].ColumnValue

				vector, err := ParseVectorBlob(vectorBlob)

				if err != nil {
					continue
				}

				var dist float64

				switch metric {
				case "L2", "l2":
					dist, _ = DistanceL2(queryVector, vector)
				case "cosine":
					dist, _ = DistanceCosine(queryVector, vector)
				case "dot":
					dist, _ = DistanceDot(queryVector, vector)
				default:
					dist, _ = DistanceL2(queryVector, vector)
				}

				pendingVectors = append(pendingVectors, VectorResult{
					RowId:    vectorID,
					Distance: dist,
				})
			}

			resultsChan <- clusterResult{vectors: pendingVectors}
		})

		// Close channel when all goroutines complete
		go func() {
			wg.Wait()
			close(resultsChan)
		}()

		// Collect results from all clusters
		for result := range resultsChan {
			if result.err != nil {
				slog.Debug("Cluster query error", "error", result.err)
				continue
			}

			for _, vec := range result.vectors {
				resultHeap.Insert(vec.RowId, vec.Distance)
			}
		}
	}

	// Extract results from heap
	return resultHeap.Results(), nil
}

// Helper functions
func sortClustersByDistance(clusters []ClusterDistance) {
	// Simple bubble sort - fine for small number of clusters
	n := len(clusters)

	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			if clusters[j].Distance > clusters[j+1].Distance {
				clusters[j], clusters[j+1] = clusters[j+1], clusters[j]
			}
		}
	}
}

// executeBruteForceSearch performs a brute-force k-NN search on pending vectors
// This is used as a fallback when no clusters exist yet (e.g., initial inserts)
func executeBruteForceSearch(dbConn *database.DatabaseConnection, resultPool *sqlite3.ResultPool, indexTableName string, queryVector *VectorBlob, k int, metric string) ([]VectorResult, error) {
	slog.Debug("Executing brute-force search", "table", indexTableName, "k", k, "metric", metric)

	// Query all pending vectors
	pendingQuery := fmt.Sprintf(`
		SELECT id, vector_blob 
		FROM %s_pending 
		WHERE operation = 'INSERT' AND vector_blob IS NOT NULL
	`, indexTableName)

	slog.Debug("Brute-force pending query", "query", pendingQuery)

	pendingResult, err := dbConn.Exec(pendingQuery, nil)

	if err != nil {
		slog.Error("Failed to query pending vectors", "error", err)
		return nil, fmt.Errorf("failed to query pending vectors: %w", err)
	}

	defer resultPool.Put(pendingResult)

	slog.Debug("Brute-force search found pending vectors", "count", len(pendingResult.Rows))

	if len(pendingResult.Rows) == 0 {
		return []VectorResult{}, nil
	}

	// Calculate distance to each pending vector
	resultHeap := NewTopKHeap(k)

	for _, row := range pendingResult.Rows {
		rowID := row[0].Int64()
		vectorBlob := row[1].ColumnValue

		vector, err := ParseVectorBlob(vectorBlob)

		if err != nil {
			slog.Warn("Failed to parse vector blob", "row_id", rowID, "error", err)
			continue
		}

		// Calculate distance based on metric
		var dist float64

		switch metric {
		case "L2", "l2":
			dist, err = DistanceL2(queryVector, vector)
		case "cosine":
			dist, err = DistanceCosine(queryVector, vector)
		case "dot":
			dist, err = DistanceDot(queryVector, vector)
		default:
			dist, err = DistanceL2(queryVector, vector)
		}

		if err != nil {
			slog.Warn("Failed to calculate distance", "row_id", rowID, "error", err)
			continue
		}

		// Use Insert() which enforces k limit, not Push() which doesn't
		resultHeap.Insert(rowID, dist)
	}

	results := resultHeap.Results()
	slog.Debug("Brute-force search returning results", "count", len(results))

	return results, nil
}
