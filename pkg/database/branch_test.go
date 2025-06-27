package database_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
)

func TestNewBranch(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		branch, err := database.NewBranch(
			app.DatabaseManager,
			"Test Branch",
		)

		if err != nil {
			t.Fatal(err)
		}

		if branch.Name != "Test Branch" {
			t.Fatal("Branch name is not correct")
		}

		if branch.BranchID == "" {
			t.Fatal("Branch ID is empty")
		}

		if branch.Key == "" {
			t.Fatal("Branch key is empty")
		}
	})
}
