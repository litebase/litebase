package database_test

import (
	"litebasedb/internal/test"
	"litebasedb/server/database"
	"testing"
)

func TestNewBranch(t *testing.T) {
	test.Run(t, func() {
		branch := database.NewBranch("Test Branch", false)

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
