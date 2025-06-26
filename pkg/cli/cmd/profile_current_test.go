package cmd_test

import (
	"errors"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/cli/config"
)

func TestProfileCurrent(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(server.App).
			WithServer(server)

		err := cli.Run("profile", "current")

		if !errors.Is(err, config.ErrorProfileNotFound) {
			t.Fatalf("expected error %v, got %v", config.ErrorProfileNotFound, err)
		}

		err = cli.Run("profile", "create", "--profile-name", "profile1", "--profile-cluster", "http://localhost:8080", "--profile-type", "access_key", "--profile-access-key-id", "test-access-key-id", "--profile-access-key-secret", "test-access-key-secret")

		if err != nil {
			t.Fatalf("expected no error when creating profile, got %v", err)
		}

		// Clear output buffer before running the command again
		cli.ClearOutput()

		err = cli.Run("profile", "current")

		if err != nil {
			t.Fatalf("expected no error when getting current profile, got %v", err)
		}

		if cli.DoesntSee("Current Profile") {
			t.Error("expected output to contain 'Current Profile'")
		}

		if cli.DoesntSee("Name") {
			t.Error("expected output to contain 'Name'")
		}

		if cli.DoesntSee("profile1") {
			t.Error("expected output to contain 'profile1'")
		}

		if cli.DoesntSee("Cluster") {
			t.Error("expected output to contain 'Cluster'")
		}

		err = cli.Run("profile", "create", "--profile-name", "profile2", "--profile-cluster", "http://localhost:8080", "--profile-type", "access_key", "--profile-access-key-id", "test-access-key-id", "--profile-access-key-secret", "test-access-key-secret")

		if err != nil {
			t.Fatalf("expected no error when creating profile, got %v", err)
		}

		// Clear output buffer before running the command again
		cli.ClearOutput()

		err = cli.Run("profile", "current")
		if err != nil {
			t.Fatalf("expected no error when getting current profile, got %v", err)
		}

		if cli.DoesntSee("profile1") {
			t.Error("expected output to contain 'profile1'")
		}

		if cli.Sees("profile2") {
			t.Error("expected output to not contain 'profile2'")
		}
	})
}
