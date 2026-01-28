package vector

import (
	"fmt"
)

// TablePartition represents a partition of a table for parallel scanning
type TablePartition struct {
	StartRow int64
	EndRow   int64
}

// PartitionTable divides a table into chunks for parallel processing
func PartitionTable(vfsID, databaseID, branchID, tableName, columnName string, queryVector *VectorBlob, k int, metric string) ([]TablePartition, error) {
	// Get connection to count rows
	conn, err := AcquireConnection(vfsID, databaseID, branchID)

	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}

	defer ReleaseConnection(conn)

	// Count total rows in the table - Exec() handles prepare and cleanup internally
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s IS NOT NULL", tableName, columnName)

	countResult, err := conn.GetConnection().Exec(countQuery, nil)

	if err != nil {
		return nil, fmt.Errorf("failed to execute count query: %w", err)
	}

	if len(countResult.Rows) == 0 || len(countResult.Rows[0]) == 0 {
		return nil, fmt.Errorf("no count result returned")
	}

	rowCount := countResult.Rows[0][0].Int64()

	if rowCount == 0 {
		return nil, nil
	}

	// Calculate optimal chunk size based on dimensions and available workers
	dimensions := queryVector.Dimensions
	chunkSize := int64(CalculateChunkSize(dimensions))

	// Create partitions that split the table among workers
	var partitions []TablePartition

	for start := int64(1); start <= rowCount; start += chunkSize {
		end := start + chunkSize - 1

		if end > rowCount {
			end = rowCount
		}

		partitions = append(partitions, TablePartition{
			StartRow: start,
			EndRow:   end,
		})
	}

	return partitions, nil
}
