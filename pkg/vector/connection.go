package vector

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/sqlite3"
	"github.com/litebase/litebase/pkg/vfs"
)

// ConnectionWrapper wraps a database connection for vector operations
type ConnectionWrapper struct {
	conn  *database.ClientConnection
	vfsID string
}

// GetConnection returns the underlying database connection
func (cw *ConnectionWrapper) GetConnection() *database.DatabaseConnection {
	return cw.conn.GetConnection()
}

// AcquireConnection gets a database connection from the connection manager
func AcquireConnection(vfsID, databaseID, branchID string) (*ConnectionWrapper, error) {
	// Get VFS instance
	vfsInstance, err := vfs.GetVfsFromId(vfsID)

	if err != nil || vfsInstance == nil {
		return nil, fmt.Errorf("VFS not found: %s", vfsID)
	}

	// Get connection manager
	connManager := vfsInstance.ConnectionManager()

	if connManager == nil {
		return nil, fmt.Errorf("connection manager not available")
	}

	// Get connection
	connInterface, err := connManager.Get(databaseID, branchID)

	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}

	// Type assert to ClientConnection (needed because interface returns any)
	conn, ok := connInterface.(*database.ClientConnection)

	if !ok {
		return nil, fmt.Errorf("connection type assertion failed: got type %T", connInterface)
	}

	return &ConnectionWrapper{
		conn:  conn,
		vfsID: vfsID,
	}, nil
}

// ReleaseConnection releases a database connection back to the pool.
// Note: This uses a hardcoded "default" VFS ID which may not exist for all connections.
// Worker connections that fail to release will be cleaned up during connection manager
// drain with a timeout. This is acceptable for worker connections used during vector scans.
func ReleaseConnection(wrapper *ConnectionWrapper) {
	if wrapper != nil && wrapper.conn != nil {
		// Get VFS instance using the stored VFS ID
		vfsInstance, err := vfs.GetVfsFromId(wrapper.vfsID)

		if err != nil || vfsInstance == nil {
			// Silently fail - worker connections will be cleaned up during drain timeout
			return
		}

		connManager := vfsInstance.ConnectionManager()

		if connManager != nil {
			connManager.Release(wrapper.conn)
		}
	}
}

// ExecuteChunkScan executes a vector scan on a chunk of the table
func ExecuteChunkScan(job *ChunkJob) (*ChunkResult, error) {
	// Get connection
	conn, err := AcquireConnection(job.VfsID, job.DatabaseID, job.BranchID)

	if err != nil {
		return nil, err
	}

	defer ReleaseConnection(conn)

	// Create heap for this chunk
	topK := NewTopKHeap(job.K)

	// Query the table chunk
	query := fmt.Sprintf("SELECT rowid, %s FROM %s WHERE rowid BETWEEN ? AND ?",
		job.ColumnName, job.TableName)

	// Use Statement() method for cached statement lookup (Phase 1 optimization)
	// This caches prepared statements per connection, avoiding repeated SQL parsing
	stmt, err := conn.GetConnection().Statement(query)

	if err != nil {
		slog.Error("Failed to prepare chunk query", "error", err)
		return nil, err
	}

	// Get a pooled result object to reuse memory allocations
	result := conn.GetConnection().ResultPool().Get()
	defer conn.GetConnection().ResultPool().Put(result)

	// Execute query with parameters
	err = stmt.Sqlite3Statement.Exec(result,
		sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: int64(job.StartRow)},
		sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: int64(job.EndRow)},
	)

	if err != nil {
		slog.Error("Failed to execute chunk query", "error", err)
		return nil, err
	}

	// Process each row from result
	for _, row := range result.Rows {
		if len(row) < 2 {
			continue
		}

		// Use the Column.Int64() method to read rowid
		rowid := row[0].Int64()

		// Get the vector blob from the second column
		vectorBlob := row[1].Blob()

		if len(vectorBlob) == 0 {
			continue
		}

		// Parse the vector BLOB using pooled allocation
		vec, err := ParseVectorBlobPooled(vectorBlob)

		if err != nil {
			slog.Debug("Failed to parse vector", "rowid", rowid, "error", err)
			continue
		}

		// Compute distance based on metric
		var distance float64

		switch job.Metric {
		case "l2":
			distance, err = DistanceL2(job.QueryVector, vec)
		case "cosine":
			distance, err = DistanceCosine(job.QueryVector, vec)
		case "dot":
			distance, err = DistanceDot(job.QueryVector, vec)
		default:
			err = fmt.Errorf("unknown metric: %s", job.Metric)
		}

		// Return VectorBlob to pool immediately after distance calculation
		PutVectorBlob(vec)

		if err != nil {
			slog.Debug("Failed to compute distance", "rowid", rowid, "error", err)
			continue
		}

		// Add to heap
		topK.Insert(rowid, distance)
	}

	return &ChunkResult{
		ChunkID: job.ChunkID,
		Heap:    topK,
		Error:   nil,
	}, nil
}

// ExecuteChunkScanWithWorker executes a vector scan using worker context
// Currently just delegates to ExecuteChunkScan - worker struct available for future optimizations
func ExecuteChunkScanWithWorker(w *Worker, job *ChunkJob) (*ChunkResult, error) {
	// Delegate to ExecuteChunkScan which now uses Statement() caching and Step() iteration
	return ExecuteChunkScan(job)
}

// ExecuteChunkScanWithWorkerOld is the old implementation kept for reference
func ExecuteChunkScanWithWorkerOld(w *Worker, job *ChunkJob) (*ChunkResult, error) {
	// Get connection for this specific database
	conn, err := AcquireConnection(job.VfsID, job.DatabaseID, job.BranchID)

	if err != nil {
		return nil, err
	}

	defer ReleaseConnection(conn)

	// Create heap for this chunk
	topK := NewTopKHeap(job.K)

	// Query the table chunk
	query := fmt.Sprintf("SELECT rowid, %s FROM %s WHERE rowid BETWEEN ? AND ?",
		job.ColumnName, job.TableName)

	stmt, err := conn.GetConnection().Prepare(context.Background(), query)

	if err != nil {
		slog.Error("Failed to prepare chunk query", "error", err)
		return nil, err
	}

	result := sqlite3.NewResult()

	err = stmt.Sqlite3Statement.Exec(result,
		sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: int64(job.StartRow)},
		sqlite3.StatementParameter{Type: sqlite3.ParameterTypeInteger, Value: int64(job.EndRow)},
	)

	if err != nil {
		slog.Error("Failed to execute chunk query", "error", err)
		return nil, err
	}

	// Calculate distance for each vector in the chunk
	for _, row := range result.Rows {
		if len(row) < 2 {
			continue
		}

		rowid := row[0].Int64()
		vectorBlob := row[1].Blob()

		if len(vectorBlob) == 0 {
			continue
		}

		// Parse the vector BLOB using pooled allocation
		vec, err := ParseVectorBlobPooled(vectorBlob)

		if err != nil {
			slog.Debug("Failed to parse vector", "rowid", rowid, "error", err)
			continue
		}

		// Compute distance based on metric
		var distance float64

		switch job.Metric {
		case "l2":
			distance, err = DistanceL2(job.QueryVector, vec)
		case "cosine":
			distance, err = DistanceCosine(job.QueryVector, vec)
		case "dot":
			distance, err = DistanceDot(job.QueryVector, vec)
		default:
			err = fmt.Errorf("unknown metric: %s", job.Metric)
		}

		// Return VectorBlob to pool immediately after distance calculation
		PutVectorBlob(vec)

		if err != nil {
			slog.Debug("Failed to compute distance", "rowid", rowid, "error", err)
			continue
		}

		// Add to heap
		topK.Insert(rowid, distance)
	}

	return &ChunkResult{
		ChunkID: job.ChunkID,
		Heap:    topK,
		Error:   nil,
	}, nil
}
