package vector

// Cluster assignment for the vector_index virtual table.
//
// goAssignVectorsInBatch    — assigns correct cluster IDs on vtab->db
//                             (no cluster_id=0 ever written)
// goUpdateClusterStats      — updates cluster_size + centroid_blob on vtab->db
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
	"sync"
	"unsafe"

	"github.com/litebase/litebase/internal/utils"
)

// clusterStmtKey identifies a cached prepared-statement entry by SQLite
// connection pointer and a table+column name pair.
type clusterStmtKey struct {
	db  uintptr
	key string // "{table}_{col}"
}

// clusterUpdateStmts holds the two persistent prepared statements used by
// goUpdateClusterStats for a specific (connection, table, column) triple.
// Preparing once and reusing across flushes avoids ~3 prepare/finalize cycles
// per cluster per flush.
type clusterUpdateStmts struct {
	// sel: SELECT centroid_blob, cluster_size … WHERE cluster_id = ?
	sel *C.sqlite3_stmt
	// upd: UPDATE … SET cluster_size = ?, centroid_blob = ? WHERE cluster_id = ?
	upd *C.sqlite3_stmt
}

// clusterUpdateStmtCache maps (db pointer, "table_col") to the persistent
// prepared statements used by goUpdateClusterStats.
var clusterUpdateStmtCache sync.Map

// clusterTreeStmtCache maps (db pointer, "table_col") to the persistent
// prepared SELECT statement used by loadClusterTree, so that the full
// cluster tree SELECT is not re-prepared on every flush.
var clusterTreeStmtCache sync.Map

// getOrPrepareClusterUpdateStmts returns (possibly cached) persistent prepared
// statements for reading and updating cluster statistics on the given connection.
func getOrPrepareClusterUpdateStmts(sdb *C.sqlite3, tbl, col string) (*clusterUpdateStmts, bool) {
	key := clusterStmtKey{db: uintptr(unsafe.Pointer(sdb)), key: tbl + "_" + col}

	if v, ok := clusterUpdateStmtCache.Load(key); ok {
		return v.(*clusterUpdateStmts), true
	}

	selSQL := fmt.Sprintf(
		"SELECT centroid_blob, cluster_size FROM %s_%s_cluster_tree WHERE cluster_id = ?",
		tbl, col,
	)

	cSelSQL := C.CString(selSQL)
	var selStmt *C.sqlite3_stmt
	rcSel := C.sqlite3_prepare_v3(sdb, cSelSQL, -1, C.SQLITE_PREPARE_PERSISTENT, &selStmt, nil)
	C.free(unsafe.Pointer(cSelSQL))

	if rcSel != C.SQLITE_OK {
		return nil, false
	}

	updSQL := fmt.Sprintf(
		"UPDATE %s_%s_cluster_tree SET cluster_size = ?, centroid_blob = ? WHERE cluster_id = ?",
		tbl, col,
	)

	cUpdSQL := C.CString(updSQL)
	var updStmt *C.sqlite3_stmt
	rcUpd := C.sqlite3_prepare_v3(sdb, cUpdSQL, -1, C.SQLITE_PREPARE_PERSISTENT, &updStmt, nil)
	C.free(unsafe.Pointer(cUpdSQL))

	if rcUpd != C.SQLITE_OK {
		C.sqlite3_finalize(selStmt)
		return nil, false
	}

	stmts := &clusterUpdateStmts{sel: selStmt, upd: updStmt}
	clusterUpdateStmtCache.Store(key, stmts)

	return stmts, true
}

// clusterNode is a minimal local copy of the cluster tree node, kept
// separate from database.ClusterNode to avoid a circular import.
type clusterNode struct {
	clusterID int64
	parentID  *int64
	centroid  []float32
	isLeaf    bool
	children  []int64
}

// loadClusterTree reads the entire cluster tree for one vector column
// using the raw sqlite3* that owns the active write transaction.  Reading
// within the same write transaction is safe in SQLite WAL mode.
//
// The underlying SELECT statement is cached per (connection, table, column)
// with SQLITE_PREPARE_PERSISTENT to avoid re-preparing on every flush.
//
// Nodes are acquired from clusterNodePool. Callers must call
// releaseClusterTree to return nodes to the pool after use.
func loadClusterTree(db *C.sqlite3, tableName, colName string) (map[int64]*clusterNode, error) {
	key := clusterStmtKey{db: uintptr(unsafe.Pointer(db)), key: tableName + "_" + colName}

	var stmt *C.sqlite3_stmt

	if v, ok := clusterTreeStmtCache.Load(key); ok {
		stmt = (*C.sqlite3_stmt)(v.(unsafe.Pointer))
		// Reset any lingering state from a previous (possibly interrupted) call.
		C.sqlite3_reset(stmt)
	} else {
		query := fmt.Sprintf(
			"SELECT cluster_id, parent_id, centroid_blob, is_leaf FROM %s_%s_cluster_tree",
			tableName, colName,
		)

		cQuery := C.CString(query)
		rc := C.sqlite3_prepare_v3(db, cQuery, -1, C.SQLITE_PREPARE_PERSISTENT, &stmt, nil)
		C.free(unsafe.Pointer(cQuery))

		if rc != C.SQLITE_OK {
			return nil, fmt.Errorf("prepare cluster tree query: rc=%d", int(rc))
		}

		clusterTreeStmtCache.Store(key, unsafe.Pointer(stmt))
	}

	// Reset at exit so the cached statement is clean for the next call.
	defer C.sqlite3_reset(stmt)

	tree := make(map[int64]*clusterNode, 64)

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

		// Parse the centroid blob without C.GoBytes: read the header
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

		node := getClusterNode()
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

// releaseClusterTree returns all nodes in tree back to the pool.
func releaseClusterTree(tree map[int64]*clusterNode) {
	for _, node := range tree {
		putClusterNode(node)
	}
}

// findBestCluster traverses the cluster tree from root (id=1) and
// returns the leaf cluster ID and distance that best match vec.
func findBestCluster(tree map[int64]*clusterNode, distMetric int, vec []float32) (int64, float64) {
	node, ok := tree[1]

	if !ok {
		return 1, 0
	}

	var dist float64

	for {
		if len(node.centroid) > 0 {
			dist = distance(vec, node.centroid, distMetric)
		}

		if node.isLeaf || len(node.children) == 0 {
			return node.clusterID, dist
		}

		var best *clusterNode
		bestDist := 1e18

		for _, childID := range node.children {
			child, ok := tree[childID]

			if !ok || len(child.centroid) == 0 {
				continue
			}

			d := distance(vec, child.centroid, distMetric)

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

// distance mirrors database.calculateDistance exactly so search and
// insert use the same metric.
//
// All accumulators use float32 so that the Go compiler can auto-vectorise
// the inner loop with NEON (ARM64) or SSE/AVX (x86-64).  Ordering is
// identical to the previous float64 version for any normally-scaled input.
func distance(a, b []float32, metric int) float64 {
	switch metric {
	case 0: // L2 (squared — monotone with actual L2, avoids sqrtf)
		var sum float32

		for i := range a {
			d := a[i] - b[i]
			sum += d * d
		}

		return float64(sum)
	case 1: // Cosine (matches database.calculateDistance: 1 - dot/(normA*normB))
		var dot, na, nb float32

		for i := range a {
			dot += a[i] * b[i]
			na += a[i] * a[i]
			nb += b[i] * b[i]
		}

		denom := float64(na) * float64(nb)

		if denom == 0 {
			return 1.0
		}

		return 1.0 - float64(dot)/denom
	case 2: // Dot product (negate so lower = closer)
		var dot float32

		for i := range a {
			dot += a[i] * b[i]
		}

		return float64(-dot)
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

	tree, err := loadClusterTree((*C.sqlite3)(db), tbl, col)

	if err != nil {
		slog.Error("goAssignVectorsInBatch: cluster tree unavailable — assigning to root",
			"table", tbl, "col", col, "error", err)

		return C.SQLITE_OK
	}

	defer releaseClusterTree(tree)

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
		cid, dist := findBestCluster(tree, metric, vec)

		clusterIDs[i] = C.sqlite3_int64(cid)
		dists[i] = C.double(dist)
	}

	return C.SQLITE_OK
}

// goUpdateClusterStats updates cluster_size and centroid_blob for each cluster
// that received vectors in this batch.  Called on vtab->db after the
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
// Prepared statements are cached per (connection, table, column) to avoid
// re-preparing on every flush (~91 times for a 1 M-vector insertion).
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

	if len(agg) == 0 {
		return C.SQLITE_OK
	}

	// Get or prepare cached persistent statements for this (connection, column).
	// Two operations per cluster: a SELECT to read the current state and a
	// single UPDATE that sets both cluster_size and centroid_blob, replacing the
	// previous three-statement per-cluster pattern (exec + prepare + prepare).
	stmts, ok := getOrPrepareClusterUpdateStmts(sdb, tbl, col)

	if !ok {
		return C.SQLITE_OK
	}

	blobSize := 6 + dims*4

	for clusterID, a := range agg {
		if a.count == 0 {
			putFloat64Slice(a.sum)
			continue
		}

		// --- Step 1: read current centroid and cluster_size ---
		C.sqlite3_reset(stmts.sel)
		C.sqlite3_bind_int64(stmts.sel, 1, C.sqlite3_int64(clusterID))

		centroidBlob := getEncodeBlob(blobSize)
		centroidBlob[0] = VectorVersion1
		centroidBlob[1] = VectorTypeFloat32
		centroidBlob[2] = byte(dims)
		centroidBlob[3] = byte(dims >> 8)
		centroidBlob[4] = byte(dims >> 16)
		centroidBlob[5] = byte(dims >> 24)

		// newCentroid is written directly into centroidBlob[6:] to avoid a
		// separate float32 slice allocation.
		newCentroid := unsafe.Slice((*float32)(unsafe.Pointer(&centroidBlob[6])), dims)

		oldSize := 0
		newSize := 0

		if C.sqlite3_step(stmts.sel) == C.SQLITE_ROW {
			oldSize = int(C.sqlite3_column_int(stmts.sel, 1))
			newSize = oldSize + a.count

			blobPtr := C.sqlite3_column_blob(stmts.sel, 0)
			blobLen := int(C.sqlite3_column_bytes(stmts.sel, 0))

			// Compute running mean while the SQLite row buffer (oldCentroid)
			// is still live — i.e. before stmts.sel is reset below.
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
						// oldCentroid points into the SQLite row buffer; safe until
						// stmts.sel is reset (which happens a few lines below).
						dataPtr := unsafe.Pointer(uintptr(base) + 6)
						oldCentroid := unsafe.Slice((*float32)(dataPtr), dims)

						for j := 0; j < dims; j++ {
							newCentroid[j] = (oldCentroid[j]*float32(oldSize) + float32(a.sum[j])) / float32(newSize)
						}
					} else {
						for j := 0; j < dims; j++ {
							newCentroid[j] = float32(a.sum[j]) / float32(newSize)
						}
					}
				} else {
					for j := 0; j < dims; j++ {
						newCentroid[j] = float32(a.sum[j]) / float32(newSize)
					}
				}
			} else {
				for j := 0; j < dims; j++ {
					newCentroid[j] = float32(a.sum[j]) / float32(newSize)
				}
			}
		}

		// Free the select row buffer before using the update statement.
		C.sqlite3_reset(stmts.sel)

		if newSize == 0 {
			putEncodeBlob(centroidBlob)
			putFloat64Slice(a.sum)
			continue
		}

		// --- Step 2: write cluster_size and centroid_blob in one UPDATE ---
		// Pass the centroid blob directly via unsafe.Pointer; SQLITE_TRANSIENT
		// makes SQLite copy the data immediately, so no C.CBytes allocation needed.
		C.sqlite3_reset(stmts.upd)
		C.sqlite3_bind_int64(stmts.upd, 1, C.sqlite3_int64(newSize))
		C.sqlite3_bind_blob(stmts.upd, 2, unsafe.Pointer(&centroidBlob[0]), C.int(blobSize), (*[0]byte)(C.SQLITE_TRANSIENT))
		C.sqlite3_bind_int64(stmts.upd, 3, C.sqlite3_int64(clusterID))

		if rc := C.sqlite3_step(stmts.upd); rc != C.SQLITE_DONE {
			slog.Error("goUpdateClusterStats: cluster update failed",
				"table", tbl, "col", col, "cluster", clusterID, "rc", int(rc))
		}

		putEncodeBlob(centroidBlob)
		putFloat64Slice(a.sum)
	}

	return C.SQLITE_OK
}

// goFinalizeClusterStmts finalizes all cached prepared statements associated
// with the given SQLite connection.  Called from the C-side xDisconnect handler
// to prevent leaking VDBE objects after the connection closes.
//
//export goFinalizeClusterStmts
func goFinalizeClusterStmts(db unsafe.Pointer) {
	dbKey := uintptr(db)

	clusterUpdateStmtCache.Range(func(k, v any) bool {
		if k.(clusterStmtKey).db == dbKey {
			stmts := v.(*clusterUpdateStmts)
			C.sqlite3_finalize(stmts.sel)
			C.sqlite3_finalize(stmts.upd)
			clusterUpdateStmtCache.Delete(k)
		}

		return true
	})

	clusterTreeStmtCache.Range(func(k, v any) bool {
		if k.(clusterStmtKey).db == dbKey {
			C.sqlite3_finalize((*C.sqlite3_stmt)(v.(unsafe.Pointer)))
			clusterTreeStmtCache.Delete(k)
		}

		return true
	})
}

// goTriggerClusterSplits registers a post-commit hook that will run cluster
// splits on the same connection that just committed.  The hook is executed by
// DatabaseConnection.Transaction() (or Exec() in auto-commit mode) after the
// SQLite commit completes and barriers are released, so the page cache is
// still warm from the insert transaction.
//
// The dbPtr is the sqlite3* pointer from the C vtab, used to correlate this
// callback with the Go DatabaseConnection that owns the pointer.
//
//export goTriggerClusterSplits
func goTriggerClusterSplits(databaseID, branchID, tableName *C.char, dbPtr C.uintptr_t) {
	mgr := GetGlobalIndexManager()

	if mgr == nil {
		return
	}

	db := C.GoString(databaseID)
	br := C.GoString(branchID)
	tbl := C.GoString(tableName)

	utils.RegisterPostCommitHook(uintptr(dbPtr), func(conn any) {
		mgr.RunSplitsWithConnection(conn, db, br, tbl)
	})
}
