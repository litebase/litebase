package backups

type SnapshotLifecycleManager struct {
	branchUuid   string
	databaseUuid string
}

func NewSnapshotLifecycleManager(databaseUuid, branchUuid string) *SnapshotLifecycleManager {
	return &SnapshotLifecycleManager{
		branchUuid:   branchUuid,
		databaseUuid: databaseUuid,
	}
}

func (s *SnapshotLifecycleManager) Reduce(timestamp int64) error {
	return nil
}
