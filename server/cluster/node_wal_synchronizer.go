package cluster

type NodeWalSynchronizer interface {
	WriteAt(databaseId, branchId string, p []byte, off, sequence, timestamp int64) error
	Truncate(databaseId, branchId string, size, sequence, timestamp int64) error
}
