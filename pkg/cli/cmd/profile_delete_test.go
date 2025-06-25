package cmd_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
)

func TestProfileDeleteCmd(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		cli := test.NewTestCLI(app)

		// Create a profile to delete
		err := cli.Run("profile", "create", "test-profile", "--profile-name", "Test Profile", "--profile-cluster", "http://localhost:8080", "--profile-type", "access_key", "--profile-access-key-id", "test-access-key-id", "--profile-access-key-secret", "test-access-key-secret")

		if err != nil {
			t.Fatalf("expected no error when creating profile, got %v", err)
		}

		// Now delete the profile
		err = cli.Run("profile", "delete", "test-profile")

		if err != nil {
			t.Fatalf("expected no error when deleting profile, got %v", err)
		}

		if !cli.ShouldSee("Profile deleted successfully") {
			t.Errorf("expected output to contain 'Profile deleted successfully', got %q", cli.GetOutput())
		}
	})
}
