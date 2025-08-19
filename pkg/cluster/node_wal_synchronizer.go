package cluster

type NodeWalSynchronizer interface {
	GetActiveWALVersions(databaseID, branchID string) ([]int64, error)
	SetCurrentTimestamp(databaseID, branchID string, timestamp int64) error
	SetWALIndexHeader(databaseID, branchID, databaseHash, nodeHash string, timestamp int64, header []byte) error
}
