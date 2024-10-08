package node

type NodeDatabaseWalSynchronizer interface {
	Sync(databaseId, branchId string, data []byte, offset int, length int, sha256 [32]byte, timestamp int64) error
	WalPath(databaseId, branchId string) string
	WalTimestamp(databaseId, branchId string) (int64, error)
}
