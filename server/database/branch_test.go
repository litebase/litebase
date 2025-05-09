package database_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server"
	"github.com/litebase/litebase/server/database"
)

func TestNewBranch(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		branch := database.NewBranch(
			app.Config,
			app.Cluster.ObjectFS(),
			"Test Branch",
			false,
		)

		if branch.Name != "Test Branch" {
			t.Fatal("Branch name is not correct")
		}

		if branch.IsPrimary {
			t.Fatal("Branch is primary")
		}

		if branch.Id == "" {
			t.Fatal("Branch ID is empty")
		}

		if branch.Key == "" {
			t.Fatal("Branch key is empty")
		}
	})
}
