package cmd_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
)

func TestAccessKeyCreate(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(server.App)

		err := cli.Run("access-key", "create")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	})
}
