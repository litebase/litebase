package cmd_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
)

func TestProfileListCmd(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		cli := test.NewTestCLI(app)

		// Create some profiles
		for i := range 10 {
			// Create a profile to delete
			err := cli.Run("profile", "create", "--profile-name", fmt.Sprintf("profile-%d", i), "--profile-cluster", "http://localhost:8080", "--profile-type", "access_key", "--profile-access-key-id", "test-access-key-id", "--profile-access-key-secret", "test-access-key-secret")

			if err != nil {
				t.Fatalf("expected no error when creating profile, got %v", err)
			}
		}

		err := cli.Run("profile", "list")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		for i := range 10 {
			profileName := fmt.Sprintf("profile-%d", i)

			if cli.DoesntSee(profileName) {
				t.Errorf("expected output to contain '%s', got %q", profileName, cli.GetOutput())
			}
		}
	})
}
