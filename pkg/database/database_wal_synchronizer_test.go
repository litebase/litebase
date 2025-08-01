package database_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
)

func TestNewDatabaseWALSynchronizer(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		synchronizer := database.NewDatabaseWALSynchronizer(app.DatabaseManager)

		if synchronizer == nil {
			t.Error("expected synchronizer to not be nil")
		}
	})
}

// func TestDatabaseWALSynchronizeTruncate(t *testing.T) {
// 	test.RunWithApp(t, func(app *server.App) {
// 		synchronizer := database.NewDatabaseWALSynchronizer(app.DatabaseManager)
// 		db := test.MockDatabase(app)

// 		err := synchronizer.Truncate(
// 			db.DatabaseID,
// 			db.DatabaseBranchID,
// 			0,
// 			0,
// 			time.Now().UTC().Unix(),
// 		)

// 		if err != nil {
// 			t.Errorf("expected error to be nil, got %v", err)
// 		}
// 	})
// }

// func TestDatabaseWALSynchronizeWriteAt(t *testing.T) {
// 	test.RunWithApp(t, func(app *server.App) {
// 		synchronizer := database.NewDatabaseWALSynchronizer(app.DatabaseManager)
// 		db := test.MockDatabase(app)

// 		err := synchronizer.WriteAt(
// 			db.DatabaseID,
// 			db.DatabaseBranchID,
// 			[]byte("hello"),
// 			0,
// 			1,
// 			time.Now().UTC().Unix(),
// 		)

// 		if err != nil {
// 			t.Errorf("expected error to be nil, got %v", err)
// 		}
// 	})
// }
