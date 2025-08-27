package cli_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/cli/config"
)

func TestCLIAuth(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		t.Run("Test No Auth", func(t *testing.T) {
			cli := test.NewTestCLI(t, server.App)

			err := cli.Run("status")

			if err != config.ErrorCredentialsNotSet {
				t.Fatalf("expected ErrorCredentialsNotSet, got %v", err)
			}
		})

		t.Run("Test Access Key Auth", func(t *testing.T) {
			cli := test.NewTestCLI(t, server.App).
				WithServer(server).
				WithAccessKey([]auth.Statement{
					{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}},
				})

			err := cli.Run("status")

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})

		t.Run("Test Basic Auth", func(t *testing.T) {
			cli := test.NewTestCLI(t, server.App).
				WithServer(server).
				WithBasicAuth("user", "password", []auth.Statement{
					{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}},
				})

			err := cli.Run("status")

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})

		t.Run("Test Token Auth", func(t *testing.T) {
			cli := test.NewTestCLI(t, server.App).
				WithServer(server).
				WithToken([]auth.Statement{
					{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}},
				})

			err := cli.Run("status")

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})
	})
}
