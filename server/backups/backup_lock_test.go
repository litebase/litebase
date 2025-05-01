package backups_test

import (
	"testing"

	"github.com/litebase/litebase/server/backups"
)

func TestBackupLock(t *testing.T) {
	lock1 := backups.GetBackupLock("test")
	lock2 := backups.GetBackupLock("test")

	if lock1 != lock2 {
		t.Error("Expected lock1 to be equal to lock2")
	}

	lock1.Lock()

	if lock1.LastLockedAt.IsZero() {
		t.Error("Expected LastLockedAt to be set")
	}

	if lock2.TryLock() {
		t.Error("Expected lock2 to not be able to lock")
	}

	lock1.Unlock()
}
