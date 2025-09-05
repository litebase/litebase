package cmd_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
)

func TestProfileDeleteCmd(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		cli := test.NewTestCLI(t, app)

		// Create a profile to delete
		err := cli.Run("profile", "create", "--profile-name", "Test Profile 1", "--profile-cluster", "http://localhost:8080", "--profile-type", "access_key", "--profile-access-key-id", "test-access-key-id", "--profile-access-key-secret", "test-access-key-secret")

		if err != nil {
			t.Fatalf("expected no error when creating profile, got %v", err)
		}

		err = cli.Run("profile", "create", "--profile-name", "Test Profile 2", "--profile-cluster", "http://localhost:8080", "--profile-type", "access_key", "--profile-access-key-id", "test-access-key-id", "--profile-access-key-secret", "test-access-key-secret")

		if err != nil {
			t.Fatalf("expected no error when creating profile, got %v", err)
		}

		err = cli.Run("profile", "create", "--profile-name", "Test Profile 3", "--profile-cluster", "http://localhost:8080", "--profile-type", "access_key", "--profile-access-key-id", "test-access-key-id", "--profile-access-key-secret", "test-access-key-secret")

		if err != nil {
			t.Fatalf("expected no error when creating profile, got %v", err)
		}

		// Now delete the profile
		err = cli.Run("profile", "delete", "Test Profile 1")

		if err != nil {
			t.Fatalf("expected no error when deleting profile, got %v", err)
		}

		if cli.DoesntSee("Profile deleted successfully") {
			t.Errorf("expected output to contain 'Profile deleted successfully', got %q", cli.GetOutput())
		}

		cli.ClearOutput()

		err = cli.Run("profile", "list")

		if err != nil {
			t.Fatalf("expected no error when listing profiles, got %v", err)
		}

		if cli.Sees("Test Profile 1") {
			t.Errorf("expected output to contain 'Test Profile 1', got %q", cli.GetOutput())
		}

		if cli.DoesntSee("Test Profile 2") {
			t.Errorf("expected output to contain 'Test Profile 2', got %q", cli.GetOutput())
		}

		if cli.DoesntSee("Test Profile 3") {
			t.Errorf("expected output to contain 'Test Profile 3', got %q", cli.GetOutput())
		}
	})
}
