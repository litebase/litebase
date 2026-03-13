package database

/*
#include <stdlib.h>
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"unsafe"

	"github.com/litebase/litebase/pkg/sqlite3"
	"github.com/litebase/litebase/pkg/vector"
)

var (
	searchResultsMap   = make(map[int64]*SearchHandle)
	searchResultsMutex sync.Mutex
	nextSearchID       int64 = 1
)

// SearchHandle holds the results of a vector search
type SearchHandle struct {
	Results []vector.VectorResult
	Index   int
}

// getVectorColumnNameForSearch queries the metadata table to get a vector column name by searching for a BLOB column
// This is a helper for search operations that need to query the vector column dynamically
func getVectorColumnNameForSearch(conn *DatabaseConnection, tableName string, columnName string) (string, error) {
	if columnName == "" {
		return "", fmt.Errorf("columnName is required")
	}

	// Fetch ALL metadata in a single query to avoid N+1 queries
	res, err := conn.Exec(
		fmt.Sprintf("SELECT key, value FROM %s_metadata WHERE key LIKE 'column_%%' OR key = 'column_count' ORDER BY key", tableName),
		nil,
	)

	if err != nil {
		return "", fmt.Errorf("failed to query metadata: %w", err)
	}

	if len(res.Rows) == 0 {
		return "", fmt.Errorf("no metadata found")
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
		return "", fmt.Errorf("no column_count metadata found")
	}

	columnCount, err := strconv.Atoi(columnCountStr)

	if err != nil {
		return "", fmt.Errorf("failed to parse column_count: %w", err)
	}

	// Search for the column by name
	for i := range columnCount {
		colName, hasName := metadata[fmt.Sprintf("column_%d_name", i)]

		if !hasName || colName != columnName {
			continue
		}

		// Verify it's a BLOB column
		colType, hasType := metadata[fmt.Sprintf("column_%d_type", i)]

		if !hasType || colType != "BLOB" {
			return "", fmt.Errorf("column %s is not a vector (BLOB) column", columnName)
		}

		return columnName, nil
	}

	return "", fmt.Errorf("column %s not found", columnName)
}

// VectorSearch performs a k-NN vector search using pre-built cluster index
// tableName should be the name of a vector_index virtual table (not a regular table)
func VectorSearch(vfsID, databaseID, branchID, tableName, columnName string, queryBlob []byte, k int) (int64, error) {
	// Parse query vector
	queryVector, err := vector.ParseVectorBlob(queryBlob)

	if err != nil {
		return -1, fmt.Errorf("failed to parse query vector: %w", err)
	}

	// Execute cluster-based search using the index's shadow tables
	// The metric is read from the table's metadata
	results, err := executeClusterSearch(vfsID, databaseID, branchID, tableName, columnName, queryVector, k)

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

// findNearestLeafClusters loads the entire cluster tree in one query and
// performs hierarchical tree traversal in memory to find the nearest leaf
// clusters.  This replaces the prior N+1 approach (one SELECT is_leaf per
// candidate per level) with a single query that loads all nodes (~214 rows for
// a 1M-vector index), building an in-memory adjacency map for O(1) child
// lookup during descent.
func findNearestLeafClusters(
	dbConn *DatabaseConnection,
	resultPool *sqlite3.ResultPool,
	indexTableName string,
	columnName string,
	queryVector *vector.VectorBlob,
	metric string,
	k int,
) ([]ClusterDistance, error) {

	// Determine how many leaf clusters to search based on k.
	numLeafClustersToSearch := 5

	if k <= 10 {
		numLeafClustersToSearch = 3
	} else if k <= 50 {
		numLeafClustersToSearch = min(8, max(5, k/10))
	} else {
		numLeafClustersToSearch = min(15, max(10, k/5))
	}

	// Step 1: Load the entire cluster tree in one query.
	// For a 1M-vector index this is ~214 rows × ~6 KB per row = ~1.3 MB.
	treeQuery := fmt.Sprintf(
		`SELECT cluster_id, parent_id, centroid_blob, is_leaf FROM %s_%s_cluster_tree`,
		indexTableName, columnName,
	)

	treeResult, err := dbConn.Exec(treeQuery, nil)

	if err != nil {
		return nil, fmt.Errorf("failed to load cluster tree: %w", err)
	}

	defer resultPool.Put(treeResult)

	if len(treeResult.Rows) == 0 {
		return nil, nil
	}

	type treeNode struct {
		clusterID int64
		isLeaf    bool
		centroid  *vector.VectorBlob
		children  []int64
	}

	nodes := make(map[int64]*treeNode, len(treeResult.Rows))
	var rootIDs []int64

	for _, row := range treeResult.Rows {
		if len(row) < 4 {
			continue
		}

		clusterID := row[0].Int64()
		isLeaf := row[3].Int64() == 1

		var centroid *vector.VectorBlob

		centroidBlob := row[2].ColumnValue

		if len(centroidBlob) > 0 {
			centroid, _ = vector.ParseVectorBlob(centroidBlob)
		}

		nodes[clusterID] = &treeNode{
			clusterID: clusterID,
			isLeaf:    isLeaf,
			centroid:  centroid,
		}

		// Track root nodes (no parent).
		if row[1].ColumnType == sqlite3.ColumnTypeNull || len(row[1].ColumnValue) == 0 {
			rootIDs = append(rootIDs, clusterID)
		}
	}

	// Wire up parent→children relationships.
	for _, row := range treeResult.Rows {
		if len(row) < 4 {
			continue
		}

		if row[1].ColumnType == sqlite3.ColumnTypeNull || len(row[1].ColumnValue) == 0 {
			continue
		}

		childID := row[0].Int64()
		parentID := row[1].Int64()

		if parent, ok := nodes[parentID]; ok {
			parent.children = append(parent.children, childID)
		}
	}

	if len(rootIDs) == 0 {
		return nil, nil
	}

	// Step 2: Compute distances to root nodes.
	computeDist := func(centroid *vector.VectorBlob) float64 {
		if centroid == nil {
			return 1e9
		}

		switch metric {
		case "L2", "l2":
			d, _ := vector.DistanceL2(queryVector, centroid)
			return d
		case "cosine":
			d, _ := vector.DistanceCosine(queryVector, centroid)
			return d
		case "dot":
			d, _ := vector.DistanceDot(queryVector, centroid)
			return d
		default:
			d, _ := vector.DistanceL2(queryVector, centroid)
			return d
		}
	}

	currentClusters := make([]ClusterDistance, 0, len(rootIDs))

	for _, rootID := range rootIDs {
		node := nodes[rootID]
		currentClusters = append(currentClusters, ClusterDistance{
			ClusterID: rootID,
			Distance:  computeDist(node.centroid),
		})
	}

	sortClustersByDistance(currentClusters)

	// Step 3: Iterative in-memory descent — no additional DB queries.
	maxDepth := 10

	for depth := 0; depth < maxDepth; depth++ {
		// Check if the top-N candidates are all leaves.
		allLeaves := true
		limit := min(numLeafClustersToSearch, len(currentClusters))

		for i := 0; i < limit; i++ {
			node, ok := nodes[currentClusters[i].ClusterID]

			if !ok || !node.isLeaf {
				allLeaves = false
				break
			}
		}

		if allLeaves {
			slog.Debug("Reached leaf clusters", "depth", depth, "num_leaves", limit)
			break
		}

		// Find the closest non-leaf cluster among all candidates.
		var closestNonLeafIdx int = -1

		for i := 0; i < len(currentClusters); i++ {
			node, ok := nodes[currentClusters[i].ClusterID]

			if ok && !node.isLeaf {
				closestNonLeafIdx = i
				break
			}
		}

		if closestNonLeafIdx < 0 {
			break
		}

		nonLeafID := currentClusters[closestNonLeafIdx].ClusterID
		nonLeafNode := nodes[nonLeafID]

		if len(nonLeafNode.children) == 0 {
			slog.Warn("Non-leaf cluster has no children", "cluster_id", nonLeafID)
			break
		}

		// Replace the non-leaf with its children.
		children := make([]ClusterDistance, 0, len(nonLeafNode.children))

		for _, childID := range nonLeafNode.children {
			child, ok := nodes[childID]

			if !ok {
				continue
			}

			children = append(children, ClusterDistance{
				ClusterID: childID,
				Distance:  computeDist(child.centroid),
			})
		}

		// Remove the expanded non-leaf; append its children.
		currentClusters = append(currentClusters[:closestNonLeafIdx], currentClusters[closestNonLeafIdx+1:]...)
		currentClusters = append(currentClusters, children...)
		sortClustersByDistance(currentClusters)
	}

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
func executeClusterSearch(vfsID, databaseID, branchID, indexTableName, columnName string, queryVector *vector.VectorBlob, k int) ([]vector.VectorResult, error) {
	// Get connection to query the index shadow tables
	conn, err := AcquireConnection(vfsID, databaseID, branchID)

	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}

	defer ReleaseConnection(conn)

	dbConn := conn.GetConnection()

	// Use the connection's result pool
	resultPool := dbConn.ResultPool()

	// Get the vector column name from metadata - verify it exists
	vectorColumn, err := getVectorColumnNameForSearch(dbConn, indexTableName, columnName)

	if err != nil {
		return nil, fmt.Errorf("failed to get vector column name: %w", err)
	}

	// Read distance metric from column metadata (not table metadata anymore)
	// Fetch ALL metadata in a single query to avoid N+1 queries
	res, err := dbConn.Exec(
		fmt.Sprintf("SELECT key, value FROM %s_metadata WHERE key LIKE 'column_%%' OR key = 'column_count' ORDER BY key", indexTableName),
		nil,
	)

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

	columnCount, err := strconv.Atoi(columnCountStr)

	if err != nil {
		return nil, fmt.Errorf("failed to parse column_count: %w", err)
	}

	// Find the column index and distance metric
	var metricValue string

	for i := range columnCount {
		colName, hasName := metadata[fmt.Sprintf("column_%d_name", i)]

		if !hasName {
			continue
		}

		if colName == vectorColumn {
			// Found the column - get its distance metric
			metricValue, ok = metadata[fmt.Sprintf("column_%d_distance_metric", i)]

			if !ok {
				return nil, fmt.Errorf("failed to get distance_metric for column %s", vectorColumn)
			}

			break
		}
	}

	if metricValue == "" {
		return nil, fmt.Errorf("distance metric not found for column %s", vectorColumn)
	}

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

	// Numeric metric for DistanceFromBlob — avoids ParseVectorBlob allocation per vector.
	metricInt, _ := strconv.Atoi(metricValue)

	// Step 1: Hierarchical tree traversal to find nearest leaf clusters for this column
	leafClusters, err := findNearestLeafClusters(dbConn, resultPool, indexTableName, vectorColumn, queryVector, metric, k)

	if err != nil {
		return nil, fmt.Errorf("failed to find nearest leaf clusters: %w", err)
	}

	if len(leafClusters) == 0 {
		// No clusters exist yet - fall back to brute-force search on pending vectors
		slog.Debug("No leaf clusters found, performing brute-force search on pending vectors", "table", indexTableName)
		return executeBruteForceSearch(dbConn, resultPool, indexTableName, vectorColumn, queryVector, k, metric)
	}

	// Step 2: Search within selected leaf clusters in parallel using goroutines
	resultHeap := vector.NewTopKHeap(k)

	if len(leafClusters) > 0 {
		// Create channels for parallel processing
		type clusterResult struct {
			vectors []vector.VectorResult
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
					INNER JOIN %s_%s_cluster_vector_map m ON v.id = m.vector_id
					WHERE m.cluster_id = ?
				`, vectorColumn, indexTableName, indexTableName, vectorColumn)

				vectorsResult, err := clusterConn.Exec(vectorsQuery, []sqlite3.StatementParameter{
					{Type: sqlite3.ParameterTypeInteger, Value: clusterID},
				})

				if err != nil {
					resultsChan <- clusterResult{err: err}
					return
				}

				defer clusterPool.Put(vectorsResult)

				// Calculate distances for all vectors in this cluster without
				// allocating a VectorBlob per row — read the float32 data from
				// the raw blob bytes directly via DistanceFromBlob.
				clusterVectors := make([]vector.VectorResult, 0, len(vectorsResult.Rows))

				for _, row := range vectorsResult.Rows {
					vectorID := row[0].Int64()

					dist, ok := vector.DistanceFromBlob(queryVector, row[1].ColumnValue, metricInt)

					if !ok {
						continue
					}

					clusterVectors = append(clusterVectors, vector.VectorResult{
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
				FROM %s_%s_cluster_vector_map
				WHERE cluster_id = 0
			`, indexTableName, vectorColumn)

			pendingCountResult, err := pendingConn.Exec(pendingCountQuery, nil)

			if err != nil {
				resultsChan <- clusterResult{err: err}
				return
			}

			defer pendingPool.Put(pendingCountResult)

			if len(pendingCountResult.Rows) == 0 || pendingCountResult.Rows[0][0].Int64() == 0 {
				resultsChan <- clusterResult{vectors: []vector.VectorResult{}}
				return
			}
			log.Println("getting rows from cluster 0")
			// Query vectors in cluster 0
			pendingQuery := fmt.Sprintf(`
				SELECT v.id, v.%s 
				FROM %s_vectors v
				INNER JOIN %s_%s_cluster_vector_map m ON v.id = m.vector_id
				WHERE m.cluster_id = 0
			`, vectorColumn, indexTableName, indexTableName, vectorColumn)

			pendingResult, err := pendingConn.Exec(pendingQuery, nil)

			if err != nil {
				resultsChan <- clusterResult{err: err}
				return
			}

			defer pendingPool.Put(pendingResult)

			pendingVectors := make([]vector.VectorResult, 0, len(pendingResult.Rows))

			for _, row := range pendingResult.Rows {
				vectorID := row[0].Int64()

				dist, ok := vector.DistanceFromBlob(queryVector, row[1].ColumnValue, metricInt)

				if !ok {
					continue
				}

				pendingVectors = append(pendingVectors, vector.VectorResult{
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
	sort.Slice(clusters, func(i, j int) bool {
		return clusters[i].Distance < clusters[j].Distance
	})
}

// executeBruteForceSearch performs a brute-force k-NN search on vectors in cluster 0
// This is used as a fallback when no proper clusters exist yet or for vectors awaiting reassignment
func executeBruteForceSearch(dbConn *DatabaseConnection, resultPool *sqlite3.ResultPool, indexTableName string, columnName string, queryVector *vector.VectorBlob, k int, metric string) ([]vector.VectorResult, error) {
	slog.Debug("Executing brute-force search", "table", indexTableName, "k", k, "metric", metric)

	// Use the provided columnName parameter directly
	vectorColumn := columnName

	// Query all vectors in cluster 0 (v2 schema)
	pendingQuery := fmt.Sprintf(`
		SELECT v.id, v.%s 
		FROM %s_vectors v
		INNER JOIN %s_%s_cluster_vector_map m ON v.id = m.vector_id
		WHERE m.cluster_id = 0
	`, vectorColumn, indexTableName, indexTableName, columnName)

	slog.Debug("Brute-force cluster 0 query", "query", pendingQuery)

	pendingResult, err := dbConn.Exec(pendingQuery, nil)

	if err != nil {
		slog.Error("Failed to query cluster 0 vectors", "error", err)
		return nil, fmt.Errorf("failed to query cluster 0 vectors: %w", err)
	}

	defer resultPool.Put(pendingResult)

	slog.Debug("Brute-force search found cluster 0 vectors", "count", len(pendingResult.Rows))

	if len(pendingResult.Rows) == 0 {
		return []vector.VectorResult{}, nil
	}

	// Numeric metric for DistanceFromBlob — avoids ParseVectorBlob allocation per vector.
	var metricInt int

	switch metric {
	case "cosine":
		metricInt = 1
	case "dot":
		metricInt = 2
	default:
		metricInt = 0
	}

	// Calculate distance to each pending vector without allocating a VectorBlob per row.
	resultHeap := vector.NewTopKHeap(k)

	for _, row := range pendingResult.Rows {
		rowID := row[0].Int64()

		dist, ok := vector.DistanceFromBlob(queryVector, row[1].ColumnValue, metricInt)

		if !ok {
			continue
		}

		// Use Insert() which enforces k limit, not Push() which doesn't
		resultHeap.Insert(rowID, dist)
	}

	results := resultHeap.Results()
	slog.Debug("Brute-force search returning results", "count", len(results))

	return results, nil
}
