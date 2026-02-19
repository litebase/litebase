package vector

// Inline cluster assignment for the vector_index virtual table.
//
// goAssignVectorsInBatch    — assigns correct cluster IDs inline on vtab->db
//                             (no cluster_id=0 ever written)
// goUpdateClusterStats      — updates cluster_size + centroid_blob inline on vtab->db
// goTriggerClusterSplits    — fires a goroutine to split oversized clusters
//                             (uses ConnectionManager, separate from vtab->db)

/*
#include "../sqlite3/sqlite3.h"
#include <stdlib.h>
#include <string.h>
*/
import "C"

import (
	"fmt"
	"log/slog"
	"unsafe"
)

// inlineClusterNode is a minimal local copy of the cluster tree node, kept
// separate from database.ClusterNode to avoid a circular import.
type inlineClusterNode struct {
	clusterID int64
	parentID  *int64
	centroid  []float32
	isLeaf    bool
	children  []int64
}

// loadInlineClusterTree reads the entire cluster tree for one vector column
// using the raw sqlite3* that owns the active write transaction.  Reading
// within the same write transaction is safe in SQLite WAL mode.
//
// Nodes are acquired from inlineClusterNodePool. Callers must call
// releaseInlineClusterTree to return nodes to the pool after use.
func loadInlineClusterTree(db *C.sqlite3, tableName, colName string) (map[int64]*inlineClusterNode, error) {
	query := fmt.Sprintf(
		"SELECT cluster_id, parent_id, centroid_blob, is_leaf FROM %s_%s_cluster_tree",
		tableName, colName,
	)

	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	var stmt *C.sqlite3_stmt

	if rc := C.sqlite3_prepare_v2(db, cQuery, -1, &stmt, nil); rc != C.SQLITE_OK {
		return nil, fmt.Errorf("prepare cluster tree query: rc=%d", int(rc))
	}

	defer C.sqlite3_finalize(stmt)

	tree := make(map[int64]*inlineClusterNode, 64)

	for C.sqlite3_step(stmt) == C.SQLITE_ROW {
		clusterID := int64(C.sqlite3_column_int64(stmt, 0))

		var parentID *int64

		if C.sqlite3_column_type(stmt, 1) == C.SQLITE_INTEGER {
			pid := int64(C.sqlite3_column_int64(stmt, 1))
			parentID = &pid
		}

		var centroid []float32

		blobPtr := C.sqlite3_column_blob(stmt, 2)
		blobLen := int(C.sqlite3_column_bytes(stmt, 2))

		// Parse the centroid blob inline without C.GoBytes: read the header
		// from the SQLite-owned buffer (valid for the lifetime of this row
		// fetch), then copy only the float32 data into a Go-owned slice that
		// outlives the statement.
		if blobLen > 6 && blobPtr != nil {
			// Read header bytes directly via unsafe pointer arithmetic.
			base := unsafe.Pointer(blobPtr)
			version := *(*byte)(base)
			vecType := *(*byte)(unsafe.Pointer(uintptr(base) + 1))

			if version == VectorVersion1 && vecType == VectorTypeFloat32 {
				// Dims are stored as little-endian uint32 at bytes 2–5.
				b2 := *(*byte)(unsafe.Pointer(uintptr(base) + 2))
				b3 := *(*byte)(unsafe.Pointer(uintptr(base) + 3))
				b4 := *(*byte)(unsafe.Pointer(uintptr(base) + 4))
				b5 := *(*byte)(unsafe.Pointer(uintptr(base) + 5))
				dims := int(uint32(b2) | uint32(b3)<<8 | uint32(b4)<<16 | uint32(b5)<<24)

				if dims > 0 && dims <= MaxDimensions && blobLen == 6+dims*4 {
					// Allocate a permanent Go slice for the centroid data.
					centroid = make([]float32, dims)
					dataPtr := unsafe.Pointer(uintptr(base) + 6)
					copy(
						unsafe.Slice((*byte)(unsafe.Pointer(&centroid[0])), dims*4),
						unsafe.Slice((*byte)(dataPtr), dims*4),
					)
				}
			}
		}

		isLeaf := C.sqlite3_column_int(stmt, 3) != 0

		node := getInlineClusterNode()
		node.clusterID = clusterID
		node.parentID = parentID
		node.centroid = centroid
		node.isLeaf = isLeaf

		tree[clusterID] = node
	}

	// Build parent→child links.
	for _, node := range tree {
		if node.parentID != nil {
			if parent, ok := tree[*node.parentID]; ok {
				parent.children = append(parent.children, node.clusterID)
			}
		}
	}

	if len(tree) == 0 {
		return nil, fmt.Errorf("cluster tree is empty for %s_%s", tableName, colName)
	}

	return tree, nil
}

// releaseInlineClusterTree returns all nodes in tree back to the pool.
func releaseInlineClusterTree(tree map[int64]*inlineClusterNode) {
	for _, node := range tree {
		putInlineClusterNode(node)
	}
}

// findInlineBestCluster traverses the cluster tree from root (id=1) and
// returns the leaf cluster ID and distance that best match vec.
func findInlineBestCluster(tree map[int64]*inlineClusterNode, distMetric int, vec []float32) (int64, float64) {
	node, ok := tree[1]

	if !ok {
		return 1, 0
	}

	var dist float64

	for {
		if len(node.centroid) > 0 {
			dist = inlineDistance(vec, node.centroid, distMetric)
		}

		if node.isLeaf || len(node.children) == 0 {
			return node.clusterID, dist
		}

		var best *inlineClusterNode
		bestDist := 1e18

		for _, childID := range node.children {
			child, ok := tree[childID]

			if !ok || len(child.centroid) == 0 {
				continue
			}

			d := inlineDistance(vec, child.centroid, distMetric)

			if d < bestDist {
				bestDist = d
				best = child
			}
		}

		if best == nil {
			return node.clusterID, dist
		}

		node = best
	}
}

// inlineDistance mirrors database.calculateDistance exactly so search and
// insert use the same metric.
func inlineDistance(a, b []float32, metric int) float64 {
	switch metric {
	case 0: // L2
		sum := 0.0

		for i := range a {
			d := float64(a[i] - b[i])
			sum += d * d
		}

		return sum
	case 1: // Cosine (matches database.calculateDistance: 1 - dot/(normA*normB))
		dot, na, nb := 0.0, 0.0, 0.0

		for i := range a {
			dot += float64(a[i] * b[i])
			na += float64(a[i] * a[i])
			nb += float64(b[i] * b[i])
		}

		denom := na * nb

		if denom == 0 {
			return 1.0
		}

		return 1.0 - dot/denom
	case 2: // Dot product (negate so lower = closer)
		dot := 0.0

		for i := range a {
			dot += float64(a[i] * b[i])
		}

		return -dot
	default:
		return 1e18
	}
}

// goAssignVectorsInBatch is exported to C and called from flush_insert_buffer
// in virtual_table_index.c after vectors are written to {table}_vectors but
// before the cluster_vector_map INSERT.  It assigns each vector to its correct
// leaf cluster so cluster_id=0 is never written.
//
// Parameters:
//
//	db            raw sqlite3* that owns the current write transaction
//	tableName     virtual table name (C string)
//	colName       vector column name (C string)
//	distMetric    distance metric constant (0=L2, 1=Cosine, 2=Dot)
//	count         number of vectors in this batch
//	blobPtrs      [count] pointers to raw vector blob data (not copied)
//	blobLens      [count] byte lengths of each blob
//	clusterIDsOut caller-allocated output array [count] — filled with cluster IDs
//	distancesOut  caller-allocated output array [count] — filled with distances
//
// Returns SQLITE_OK (0).  On error (e.g. empty tree on first insert) falls
// back to assigning all vectors to cluster 1 (root) so inserts never fail.
//
//export goAssignVectorsInBatch
func goAssignVectorsInBatch(
	db unsafe.Pointer,
	tableName *C.char,
	colName *C.char,
	distMetric C.int,
	count C.int,
	blobPtrs *unsafe.Pointer,
	blobLens *C.int,
	clusterIDsOut *C.sqlite3_int64,
	distancesOut *C.double,
) C.int {
	n := int(count)

	if n == 0 {
		return C.SQLITE_OK
	}

	tbl := C.GoString(tableName)
	col := C.GoString(colName)
	metric := int(distMetric)

	clusterIDs := unsafe.Slice(clusterIDsOut, n)
	dists := unsafe.Slice(distancesOut, n)

	// Default: assign to root cluster (1) in case tree load fails or blob is invalid.
	for i := range clusterIDs {
		clusterIDs[i] = 1
		dists[i] = 0
	}

	tree, err := loadInlineClusterTree((*C.sqlite3)(db), tbl, col)

	if err != nil {
		slog.Error("goAssignVectorsInBatch: cluster tree unavailable — assigning to root",
			"table", tbl, "col", col, "error", err)

		return C.SQLITE_OK
	}

	defer releaseInlineClusterTree(tree)

	ptrs := unsafe.Slice(blobPtrs, n)
	lens := unsafe.Slice(blobLens, n)

	for i := 0; i < n; i++ {
		blen := int(lens[i])

		if blen <= 6 || ptrs[i] == nil {
			continue
		}

		// Read the vector blob directly from the C pointer without copying.
		// The pointer is valid for the lifetime of this CGo call.
		base := ptrs[i]
		version := *(*byte)(base)
		vecType := *(*byte)(unsafe.Pointer(uintptr(base) + 1))

		if version != VectorVersion1 || vecType != VectorTypeFloat32 {
			continue
		}

		b2 := *(*byte)(unsafe.Pointer(uintptr(base) + 2))
		b3 := *(*byte)(unsafe.Pointer(uintptr(base) + 3))
		b4 := *(*byte)(unsafe.Pointer(uintptr(base) + 4))
		b5 := *(*byte)(unsafe.Pointer(uintptr(base) + 5))
		dims := int(uint32(b2) | uint32(b3)<<8 | uint32(b4)<<16 | uint32(b5)<<24)

		if dims <= 0 || dims > MaxDimensions || blen != 6+dims*4 {
			continue
		}

		// vec points directly into C memory; valid until the function returns.
		vec := unsafe.Slice((*float32)(unsafe.Pointer(uintptr(base)+6)), dims)
		cid, dist := findInlineBestCluster(tree, metric, vec)

		clusterIDs[i] = C.sqlite3_int64(cid)
		dists[i] = C.double(dist)
	}

	return C.SQLITE_OK
}

// goUpdateClusterStats updates cluster_size and centroid_blob for each cluster
// that received vectors in this batch.  Called inline on vtab->db after the
// cluster_vector_map INSERT so the changes are part of the same transaction.
//
// This function receives one entry per vector in the batch (not pre-aggregated):
//
//	db            raw sqlite3* owning the write transaction
//	tableName     virtual table name
//	colName       vector column name
//	dimensions    vector dimensionality
//	numVectors    length of the per-row arrays
//	clusterIDs    per-row cluster ID assigned to each vector
//	blobLens      per-row byte length of each vector blob (0 = skip row)
//	vectorBlobs   per-row raw vector blob pointers
//	_unused       unused (same as blobLens — kept for C signature compat)
//
// The function aggregates per-cluster then updates cluster_size + centroid_blob.
//
//export goUpdateClusterStats
func goUpdateClusterStats(
	db unsafe.Pointer,
	tableName *C.char,
	colName *C.char,
	dimensions C.int,
	numVectors C.int,
	clusterIDs *C.sqlite3_int64,
	blobLens *C.int,
	vectorBlobs *unsafe.Pointer,
	_ *C.int,
) C.int {
	n := int(numVectors)

	if n == 0 {
		return C.SQLITE_OK
	}

	tbl := C.GoString(tableName)
	col := C.GoString(colName)
	dims := int(dimensions)
	sdb := (*C.sqlite3)(db)

	cids := unsafe.Slice(clusterIDs, n)
	clens := unsafe.Slice(blobLens, n)
	ptrs := unsafe.Slice(vectorBlobs, n)

	// Aggregate per cluster: sum of vectors and count.
	type clusterAgg struct {
		count int
		sum   []float64
	}

	agg := make(map[int64]*clusterAgg, 8)

	for i := 0; i < n; i++ {
		cid := int64(cids[i])
		blen := int(clens[i])

		if blen <= 6 || ptrs[i] == nil {
			continue
		}

		// Read the vector blob directly from the C pointer without copying.
		// The pointer is valid for the lifetime of this CGo call.
		base := ptrs[i]
		version := *(*byte)(base)
		vecType := *(*byte)(unsafe.Pointer(uintptr(base) + 1))

		if version != VectorVersion1 || vecType != VectorTypeFloat32 {
			continue
		}

		b2 := *(*byte)(unsafe.Pointer(uintptr(base) + 2))
		b3 := *(*byte)(unsafe.Pointer(uintptr(base) + 3))
		b4 := *(*byte)(unsafe.Pointer(uintptr(base) + 4))
		b5 := *(*byte)(unsafe.Pointer(uintptr(base) + 5))
		vdims := int(uint32(b2) | uint32(b3)<<8 | uint32(b4)<<16 | uint32(b5)<<24)

		if vdims != dims || blen != 6+dims*4 {
			continue
		}

		// vec points directly into C memory; valid until the function returns.
		vec := unsafe.Slice((*float32)(unsafe.Pointer(uintptr(base)+6)), dims)

		a, ok := agg[cid]

		if !ok {
			a = &clusterAgg{sum: getFloat64Slice(dims)}
			agg[cid] = a
		}

		a.count++

		for j, v := range vec {
			a.sum[j] += float64(v)
		}
	}

	for clusterID, a := range agg {
		if a.count == 0 {
			continue
		}

		// Increment cluster_size.
		sizeSQL := fmt.Sprintf(
			"UPDATE %s_%s_cluster_tree SET cluster_size = cluster_size + %d WHERE cluster_id = %d",
			tbl, col, a.count, clusterID,
		)

		cSizeSQL := C.CString(sizeSQL)

		if rc := C.sqlite3_exec(sdb, cSizeSQL, nil, nil, nil); rc != C.SQLITE_OK {
			C.free(unsafe.Pointer(cSizeSQL))
			slog.Error("goUpdateClusterStats: cluster_size update failed",
				"table", tbl, "col", col, "cluster", clusterID, "rc", int(rc))
			putFloat64Slice(a.sum)
			continue
		}

		C.free(unsafe.Pointer(cSizeSQL))

		// Read current centroid + (now-updated) size to recompute running mean.
		fetchSQL := fmt.Sprintf(
			"SELECT centroid_blob, cluster_size FROM %s_%s_cluster_tree WHERE cluster_id = %d",
			tbl, col, clusterID,
		)

		cFetchSQL := C.CString(fetchSQL)
		var fetchStmt *C.sqlite3_stmt
		rc2 := C.sqlite3_prepare_v2(sdb, cFetchSQL, -1, &fetchStmt, nil)
		C.free(unsafe.Pointer(cFetchSQL))

		if rc2 != C.SQLITE_OK {
			putFloat64Slice(a.sum)
			continue
		}

		var oldCentroid []float32
		var newSize int

		if C.sqlite3_step(fetchStmt) == C.SQLITE_ROW {
			blobPtr := C.sqlite3_column_blob(fetchStmt, 0)
			blobLen := int(C.sqlite3_column_bytes(fetchStmt, 0))
			newSize = int(C.sqlite3_column_int(fetchStmt, 1))

			// Read centroid inline without C.GoBytes: interpret the SQLite blob
			// pointer directly for the duration of this row fetch.
			if blobLen > 6 && blobPtr != nil {
				base := unsafe.Pointer(blobPtr)
				version := *(*byte)(base)
				vecType := *(*byte)(unsafe.Pointer(uintptr(base) + 1))

				if version == VectorVersion1 && vecType == VectorTypeFloat32 {
					b2 := *(*byte)(unsafe.Pointer(uintptr(base) + 2))
					b3 := *(*byte)(unsafe.Pointer(uintptr(base) + 3))
					b4 := *(*byte)(unsafe.Pointer(uintptr(base) + 4))
					b5 := *(*byte)(unsafe.Pointer(uintptr(base) + 5))
					cdims := int(uint32(b2) | uint32(b3)<<8 | uint32(b4)<<16 | uint32(b5)<<24)

					if cdims == dims && blobLen == 6+dims*4 {
						dataPtr := unsafe.Pointer(uintptr(base) + 6)
						// Point directly into the SQLite buffer; valid only
						// until sqlite3_finalize below.
						oldCentroid = unsafe.Slice((*float32)(dataPtr), dims)
					}
				}
			}
		}

		if newSize == 0 {
			C.sqlite3_finalize(fetchStmt)
			putFloat64Slice(a.sum)
			continue
		}

		// Running mean: new_centroid = (old * oldSize + sumOfNewVecs) / newSize
		oldSize := newSize - a.count
		blobSize := 6 + dims*4
		centroidBlob := getEncodeBlob(blobSize)

		// Write the vector blob header into the pooled buffer.
		centroidBlob[0] = VectorVersion1
		centroidBlob[1] = VectorTypeFloat32
		centroidBlob[2] = byte(dims)
		centroidBlob[3] = byte(dims >> 8)
		centroidBlob[4] = byte(dims >> 16)
		centroidBlob[5] = byte(dims >> 24)

		// newCentroid is written directly into centroidBlob[6:] to avoid a
		// separate float32 slice allocation.
		newCentroid := unsafe.Slice((*float32)(unsafe.Pointer(&centroidBlob[6])), dims)

		if len(oldCentroid) == dims {
			for j := 0; j < dims; j++ {
				newCentroid[j] = (oldCentroid[j]*float32(oldSize) + float32(a.sum[j])) / float32(newSize)
			}
		} else {
			for j := 0; j < dims; j++ {
				newCentroid[j] = float32(a.sum[j]) / float32(newSize)
			}
		}

		// Finalize the fetch statement now that we have copied all data from
		// the SQLite-owned buffer (oldCentroid slice above).
		C.sqlite3_finalize(fetchStmt)

		updateSQL := fmt.Sprintf(
			"UPDATE %s_%s_cluster_tree SET centroid_blob = ? WHERE cluster_id = %d",
			tbl, col, clusterID,
		)

		cUpdateSQL := C.CString(updateSQL)
		var updateStmt *C.sqlite3_stmt

		if C.sqlite3_prepare_v2(sdb, cUpdateSQL, -1, &updateStmt, nil) == C.SQLITE_OK {
			cBlob := C.CBytes(centroidBlob)
			C.sqlite3_bind_blob(updateStmt, 1, cBlob, C.int(len(centroidBlob)), (*[0]byte)(C.SQLITE_TRANSIENT))
			C.sqlite3_step(updateStmt)
			C.sqlite3_finalize(updateStmt)
			C.free(cBlob)
		}

		C.free(unsafe.Pointer(cUpdateSQL))
		putEncodeBlob(centroidBlob)
		putFloat64Slice(a.sum)
	}

	return C.SQLITE_OK
}

// goTriggerClusterSplits fires a goroutine to split oversized clusters after
// the current transaction commits.  Uses the VectorIndexManager's inline path
// which obtains its own connection.
//
//export goTriggerClusterSplits
func goTriggerClusterSplits(databaseID, branchID, tableName *C.char) {
	mgr := GetGlobalIndexManager()

	if mgr != nil {
		db := C.GoString(databaseID)
		br := C.GoString(branchID)
		tbl := C.GoString(tableName)
		go mgr.RunSplits(db, br, tbl)
	}
}
