package database_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
)

func TestSystemDatabase(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		db := server.App.DatabaseManager.SystemDatabase()

		if db == nil {
			t.Fatal("expected system database to be initialized")
		}

		_, err := db.Exec("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT)", nil)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		db.Close()
	})
}
