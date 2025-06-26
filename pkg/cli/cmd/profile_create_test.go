package cmd_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
)

func TestProfileCreateCmd(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		cli := test.NewTestCLI(app)

		err := cli.Run("profile", "create", "--profile-name", "Test Profile", "--profile-cluster", "http://localhost:8080", "--profile-type", "access_key", "--profile-access-key-id", "test-access-key-id", "--profile-access-key-secret", "test-access-key-secret")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesntSee("Profile stored successfully") {
			t.Errorf("expected output to contain 'Profile stored successfully', got %q", cli.GetOutput())
		}
	})
}
