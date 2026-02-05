package vector

/*
#include <stdlib.h>
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"log"
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

// getVectorColumnNameForSearch queries the metadata table to get the first vector column name
// This is a helper for search operations that need to query the vector column dynamically
func getVectorColumnNameForSearch(conn *database.DatabaseConnection, tableName string) (string, error) {
	res, err := conn.Exec(
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

// findNearestLeafClusters performs hierarchical tree traversal to find nearest leaf clusters
// This implements the IVF hierarchical search: traverse from root to leaves, then return closest leaf clusters
func findNearestLeafClusters(
	dbConn *database.DatabaseConnection,
	resultPool *sqlite3.ResultPool,
	indexTableName string,
	queryVector *VectorBlob,
	metric string,
	k int,
) ([]ClusterDistance, error) {

	// Determine how many leaf clusters to search based on k
	// Balance between speed and recall:
	// - For small k (<=10): search 3-5 leaf clusters to ensure good recall
	// - For medium k (11-50): search 5-8 leaf clusters
	// - For large k (>50): search 10+ leaf clusters
	// With ~3k vectors per leaf cluster, searching 5 clusters = ~15k vectors
	numLeafClustersToSearch := 5 // Default: search 5 nearest leaf clusters

	if k <= 10 {
		numLeafClustersToSearch = 3
	} else if k <= 50 {
		numLeafClustersToSearch = min(8, max(5, k/10))
	} else {
		numLeafClustersToSearch = min(15, max(10, k/5))
	}

	// Step 1: Find root clusters (parent_id IS NULL)
	rootQuery := fmt.Sprintf(`
		SELECT cluster_id, centroid_blob, is_leaf
		FROM %s_cluster_tree
		WHERE parent_id IS NULL
	`, indexTableName)

	rootResult, err := dbConn.Exec(rootQuery, nil)

	if err != nil {
		return nil, fmt.Errorf("failed to query root clusters: %w", err)
	}

	defer resultPool.Put(rootResult)

	if len(rootResult.Rows) == 0 {
		return nil, nil // No clusters exist yet
	}

	// Step 2: Compute distances to root centroids
	var currentClusters []ClusterDistance

	for _, row := range rootResult.Rows {
		clusterID := row[0].Int64()
		centroidBlob := row[1].ColumnValue
		isLeaf := row[2].Int64()

		centroid, err := ParseVectorBlob(centroidBlob)

		if err != nil {
			slog.Warn("Failed to parse root centroid blob", "cluster_id", clusterID, "error", err)
			continue
		}

		var dist float64

		switch metric {
		case "L2", "l2":
			dist, _ = DistanceL2(queryVector, centroid)
		case "cosine":
			dist, _ = DistanceCosine(queryVector, centroid)
		case "dot":
			dist, _ = DistanceDot(queryVector, centroid)
		default:
			dist, _ = DistanceL2(queryVector, centroid)
		}

		// If this is a leaf node, add it to current clusters
		// If it's an internal node, we'll traverse its children
		currentClusters = append(currentClusters, ClusterDistance{
			ClusterID: clusterID,
			Distance:  dist,
		})

		// Check if root is a leaf (single-level tree)
		if isLeaf == 1 {
			slog.Debug("Root cluster is a leaf (flat structure)", "cluster_id", clusterID)
		}
	}

	// Sort by distance to find nearest clusters
	sortClustersByDistance(currentClusters)

	// Step 3: Iterative tree descent - traverse until we reach leaf clusters
	// Keep track of closest nodes at each level, then descend into closest parent
	maxDepth := 10 // Prevent infinite loops in case of circular references

	for depth := 0; depth < maxDepth; depth++ {
		// Check if all current clusters are leaves
		allLeaves := true

		for i := 0; i < min(numLeafClustersToSearch, len(currentClusters)); i++ {
			cluster := currentClusters[i]

			// Check if this cluster is a leaf
			leafCheckQuery := fmt.Sprintf(`
				SELECT is_leaf 
				FROM %s_cluster_tree 
				WHERE cluster_id = ?
			`, indexTableName)

			leafCheckResult, err := dbConn.Exec(leafCheckQuery, []sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeInteger, Value: cluster.ClusterID},
			})

			if err != nil {
				slog.Warn("Failed to check if cluster is leaf", "cluster_id", cluster.ClusterID, "error", err)
				continue
			}

			if len(leafCheckResult.Rows) > 0 {
				isLeaf := leafCheckResult.Rows[0][0].Int64()

				if isLeaf == 0 {
					allLeaves = false
				}
			}

			resultPool.Put(leafCheckResult)

			if !allLeaves {
				break
			}
		}

		// If all top candidates are leaves, we're done
		if allLeaves {
			slog.Debug("Reached leaf clusters", "depth", depth, "num_leaves", min(numLeafClustersToSearch, len(currentClusters)))
			break
		}

		// Find the closest non-leaf cluster and descend into its children
		var closestNonLeaf *ClusterDistance

		for i := 0; i < len(currentClusters); i++ {
			cluster := currentClusters[i]

			leafCheckQuery := fmt.Sprintf(`
				SELECT is_leaf 
				FROM %s_cluster_tree 
				WHERE cluster_id = ?
			`, indexTableName)

			leafCheckResult, err := dbConn.Exec(leafCheckQuery, []sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeInteger, Value: cluster.ClusterID},
			})

			if err != nil {
				slog.Warn("Failed to check cluster leaf status", "cluster_id", cluster.ClusterID, "error", err)
				resultPool.Put(leafCheckResult)
				continue
			}

			if len(leafCheckResult.Rows) > 0 && leafCheckResult.Rows[0][0].Int64() == 0 {
				resultPool.Put(leafCheckResult)
				closestNonLeaf = &currentClusters[i]
				break
			}

			resultPool.Put(leafCheckResult)
		}

		if closestNonLeaf == nil {
			// All clusters are leaves
			break
		}

		// Query children of the closest non-leaf cluster
		childrenQuery := fmt.Sprintf(`
			SELECT cluster_id, centroid_blob 
			FROM %s_cluster_tree 
			WHERE parent_id = ?
		`, indexTableName)

		childrenResult, err := dbConn.Exec(childrenQuery, []sqlite3.StatementParameter{
			{Type: sqlite3.ParameterTypeInteger, Value: closestNonLeaf.ClusterID},
		})

		if err != nil {
			return nil, fmt.Errorf("failed to query child clusters: %w", err)
		}

		// Compute distances to children
		var children []ClusterDistance

		for _, row := range childrenResult.Rows {
			childID := row[0].Int64()
			centroidBlob := row[1].ColumnValue

			centroid, err := ParseVectorBlob(centroidBlob)

			if err != nil {
				continue
			}

			var dist float64

			switch metric {
			case "L2", "l2":
				dist, _ = DistanceL2(queryVector, centroid)
			case "cosine":
				dist, _ = DistanceCosine(queryVector, centroid)
			case "dot":
				dist, _ = DistanceDot(queryVector, centroid)
			default:
				dist, _ = DistanceL2(queryVector, centroid)
			}

			children = append(children, ClusterDistance{
				ClusterID: childID,
				Distance:  dist,
			})
		}

		resultPool.Put(childrenResult)

		if len(children) == 0 {
			slog.Warn("Non-leaf cluster has no children", "cluster_id", closestNonLeaf.ClusterID)
			break
		}

		// Replace parent with its children in the candidate list
		// Remove the parent, add children, then resort
		currentClusters = append(currentClusters[:0], currentClusters[1:]...) // Remove first element (the parent)

		currentClusters = append(currentClusters, children...)
		sortClustersByDistance(currentClusters)
	}

	// Return top N leaf clusters
	numToReturn := min(numLeafClustersToSearch, len(currentClusters))

	return currentClusters[:numToReturn], nil
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

	// If there's an active transaction on this database+branch, return an error
	// rather than acquiring new connections which can deadlock.
	if database.IsTransactionActive(databaseID, branchID) {
		slog.Debug("vector_search invoked inside active transaction", "database", databaseID, "branch", branchID)

		return C.longlong(-1)
	}

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

// executeClusterSearch performs k-NN search using hierarchical IVF cluster index
// This traverses the cluster tree from root to leaves, then searches leaf cluster members
func executeClusterSearch(vfsID, databaseID, branchID, indexTableName string, queryVector *VectorBlob, k int) ([]VectorResult, error) {
	log.Println("executeClusterSearch")
	// Get connection to query the index shadow tables
	conn, err := AcquireConnection(vfsID, databaseID, branchID)

	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}

	defer ReleaseConnection(conn)

	dbConn := conn.GetConnection()

	// Use the connection's result pool
	resultPool := dbConn.ResultPool()

	// Get the vector column name from metadata
	vectorColumn, err := getVectorColumnNameForSearch(dbConn, indexTableName)

	if err != nil {
		return nil, fmt.Errorf("failed to get vector column name: %w", err)
	}

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

	// Step 1: Hierarchical tree traversal to find nearest leaf clusters
	leafClusters, err := findNearestLeafClusters(dbConn, resultPool, indexTableName, queryVector, metric, k)

	if err != nil {
		return nil, fmt.Errorf("failed to find nearest leaf clusters: %w", err)
	}

	if len(leafClusters) == 0 {
		// No clusters exist yet - fall back to brute-force search on pending vectors
		slog.Debug("No leaf clusters found, performing brute-force search on pending vectors", "table", indexTableName)
		return executeBruteForceSearch(dbConn, resultPool, indexTableName, queryVector, k, metric)
	}

	// Step 2: Search within selected leaf clusters in parallel using goroutines
	resultHeap := NewTopKHeap(k)

	if len(leafClusters) > 0 {
		// Create channels for parallel processing
		type clusterResult struct {
			vectors []VectorResult
			err     error
		}

		resultsChan := make(chan clusterResult, len(leafClusters)+1) // +1 for pending vectors
		var wg sync.WaitGroup

		// Query each leaf cluster in parallel
		for _, cluster := range leafClusters {
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

				// Query vectors in this leaf cluster via mapping table + vectors table
				vectorsQuery := fmt.Sprintf(`
					SELECT v.id, v.%s 
					FROM %s_vectors v
					INNER JOIN %s_cluster_vector_map m ON v.id = m.vector_id
					WHERE m.cluster_id = ?
				`, vectorColumn, indexTableName, indexTableName)

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

			// Check if there are vectors in cluster 0 (awaiting reassignment)
			pendingCountQuery := fmt.Sprintf(`
				SELECT COUNT(*) 
				FROM %s_cluster_vector_map
				WHERE cluster_id = 0
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
			log.Println("getting rows from cluster 0")
			// Query vectors in cluster 0
			pendingQuery := fmt.Sprintf(`
				SELECT v.id, v.%s 
				FROM %s_vectors v
				INNER JOIN %s_cluster_vector_map m ON v.id = m.vector_id
				WHERE m.cluster_id = 0
			`, vectorColumn, indexTableName, indexTableName)

			pendingResult, err := pendingConn.Exec(pendingQuery, nil)

			if err != nil {
				resultsChan <- clusterResult{err: err}
				return
			}

			defer pendingPool.Put(pendingResult)

			pendingVectors := make([]VectorResult, 0, len(pendingResult.Rows))

			for _, row := range pendingResult.Rows {
				log.Printf("processing row %d \n", row[0].Int64())
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

// executeBruteForceSearch performs a brute-force k-NN search on vectors in cluster 0
// This is used as a fallback when no proper clusters exist yet or for vectors awaiting reassignment
func executeBruteForceSearch(dbConn *database.DatabaseConnection, resultPool *sqlite3.ResultPool, indexTableName string, queryVector *VectorBlob, k int, metric string) ([]VectorResult, error) {
	slog.Debug("Executing brute-force search", "table", indexTableName, "k", k, "metric", metric)

	// Get the vector column name from metadata
	vectorColumn, err := getVectorColumnNameForSearch(dbConn, indexTableName)

	if err != nil {
		return nil, fmt.Errorf("failed to get vector column name: %w", err)
	}

	// Query all vectors in cluster 0 (v2 schema)
	pendingQuery := fmt.Sprintf(`
		SELECT v.id, v.%s 
		FROM %s_vectors v
		INNER JOIN %s_cluster_vector_map m ON v.id = m.vector_id
		WHERE m.cluster_id = 0
	`, vectorColumn, indexTableName, indexTableName)

	slog.Debug("Brute-force cluster 0 query", "query", pendingQuery)

	pendingResult, err := dbConn.Exec(pendingQuery, nil)

	if err != nil {
		slog.Error("Failed to query cluster 0 vectors", "error", err)
		return nil, fmt.Errorf("failed to query cluster 0 vectors: %w", err)
	}

	defer resultPool.Put(pendingResult)

	slog.Debug("Brute-force search found cluster 0 vectors", "count", len(pendingResult.Rows))

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
