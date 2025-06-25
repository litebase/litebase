package cmd_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
)

func TestProfileSwitch(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(server.App).
			WithServer(server)

		err := cli.Run("profile", "create", "--profile-name", "profile1", "--profile-cluster", "http://localhost:8080", "--profile-type", "access_key", "--profile-access-key-id", "test-access-key-id", "--profile-access-key-secret", "test-access-key-secret")

		if err != nil {
			t.Fatalf("expected no error when creating profile, got %v", err)
		}

		err = cli.Run("profile", "create", "--profile-name", "profile2", "--profile-cluster", "http://localhost:8080", "--profile-type", "access_key", "--profile-access-key-id", "test-access-key-id", "--profile-access-key-secret", "test-access-key-secret")

		if err != nil {
			t.Fatalf("expected no error when creating profile, got %v", err)
		}

		err = cli.Run("profile", "switch", "profile2")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if !cli.ShouldSee("Profile switched successfully to 'profile2'") {
			t.Errorf("expected output to contain 'Profile switched successfully to 'profile2'', got %q", cli.GetOutput())
		}
	})
}
