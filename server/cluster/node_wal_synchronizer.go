package cluster

type NodeWalSynchronizer interface {
	GetActiveWALVersions(databaseId, branchId string) ([]int64, error)
	SetCurrentTimestamp(databaseId, branchId string, timestamp int64) error
	SetWALIndexHeader(databaseId, branchId string, header []byte) error
}
