package cmd_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestDatabaseList(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(server.App).
			WithServer(server).
			WithAccessKey([]auth.AccessKeyStatement{
				{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		// Create a couple of databases
		database1 := test.MockDatabase(server.App)
		database2 := test.MockDatabase(server.App)

		err := cli.Run("database", "list")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesntSee(database1.DatabaseID) {
			t.Error("expected output to contain database1 ID")
		}

		if cli.DoesntSee(database2.DatabaseID) {
			t.Errorf("expected output to contain database2 ID, got %v", cli.GetOutput())
		}

		t.Log("Database list output:", cli.GetOutput())
	})
}
