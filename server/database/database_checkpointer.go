package database

type DatabaseCheckpointer struct {
}

func NewDatabaseCheckpointer() *DatabaseCheckpointer {
	return &DatabaseCheckpointer{}
}

func (d *DatabaseCheckpointer) CheckpointReplica(databaseId, branchId string, timesatmp int64) error {
	// Since we are on the replica, we don't need to perform a SQLite checkpoint.
	// Instead, we can just create a new WAL version with the timestamp.
	err := CreateWalVersion(databaseId, branchId, timesatmp)

	if err != nil {
		return err
	}

	// ConnectionManager().CheckpointReplica(databaseId, branchId, timesatmp)

	return nil
}
