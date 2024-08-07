package node

type NodeDatabaseWalSynchronizer interface {
	Sync(databaseUuid, branchUuid string, data []byte, offset int, length int, sha256 [32]byte, timestamp int64) error
	WalPath(databaseUuid, branchUuid string) string
	WalTimestamp(databaseUuid, branchUuid string) (int64, error)
}
