package auth_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/server"
)

func TestDatabaseKey(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("NewDatabaseKey", func(t *testing.T) {
			mock := test.MockDatabase(app)

			databaseKey := auth.NewDatabaseKey(
				mock.DatabaseID,
				mock.DatabaseName,
				mock.DatabaseBranchID,
				mock.BranchName,
			)

			if databaseKey == nil {
				t.Fatal("database key is nil")
			}

			if databaseKey.DatabaseID != mock.DatabaseID {
				t.Errorf("expected DatabaseID %s, got %s", mock.DatabaseID, databaseKey.DatabaseID)
			}

			if databaseKey.DatabaseName != mock.DatabaseName {
				t.Errorf("expected DatabaseName %s, got %s", mock.DatabaseName, databaseKey.DatabaseName)
			}

			if databaseKey.DatabaseBranchID != mock.DatabaseBranchID {
				t.Errorf("expected BranchID %s, got %s", mock.DatabaseBranchID, databaseKey.DatabaseBranchID)
			}

			if databaseKey.DatabaseBranchName != mock.BranchName {
				t.Errorf("expected BranchName %s, got %s", mock.BranchName, databaseKey.DatabaseBranchName)
			}
		})
	})
}
