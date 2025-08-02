package cmd_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestDatabaseShow(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(server.App).
			WithServer(server).
			WithAccessKey([]auth.AccessKeyStatement{
				{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		database := test.MockDatabase(server.App)

		err := cli.Run("database", "show", database.DatabaseName)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if !cli.Sees(database.DatabaseID) {
			t.Errorf("expected output to contain database ID %s", database.DatabaseID)
		}

		if !cli.Sees(database.DatabaseName) {
			t.Errorf("expected output to contain database name %s", database.DatabaseName)
		}
	})
}
