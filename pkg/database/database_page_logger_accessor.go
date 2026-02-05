package database

// DatabasePageLoggerAccessor implements cluster.NodePageLoggerAccessor
type DatabasePageLoggerAccessor struct {
	databaseManager *DatabaseManager
}

// NewDatabasePageLoggerAccessor creates a new DatabasePageLoggerAccessor
func NewDatabasePageLoggerAccessor(databaseManager *DatabaseManager) *DatabasePageLoggerAccessor {
	return &DatabasePageLoggerAccessor{
		databaseManager: databaseManager,
	}
}

// GetPageLoggerInUseVersions implements cluster.NodePageLoggerAccessor
func (dpla *DatabasePageLoggerAccessor) GetPageLoggerInUseVersions(databaseID, branchID string) ([]int64, error) {
	pageLogger := dpla.databaseManager.PageLogManager().Get(
		databaseID,
		branchID,
		dpla.databaseManager.Cluster.NetworkFS(),
	)

	if pageLogger == nil {
		return []int64{}, nil
	}

	return pageLogger.GetInUseVersions(), nil
}
