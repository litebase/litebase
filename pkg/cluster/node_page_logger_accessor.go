package cluster

type NodePageLoggerAccessor interface {
	GetPageLoggerInUseVersions(databaseID, branchID string) ([]int64, error)
}
