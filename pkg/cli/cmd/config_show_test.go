package cmd_test

import (
	"os"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/cli/config"
	"go.yaml.in/yaml/v4"
)

func TestConfigShow(t *testing.T) {
	test.Run(t, func() {
		t.Run("show config", func(t *testing.T) {
			cli := test.NewTestCLI(t, nil)

			err := cli.Run("config", "show")

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			if cli.DoesNotSee("Litebase Server Config") {
				t.Error("expected output to contain 'Litebase Server Config'")
			}

			if cli.DoesNotSee("Cluster ID") {
				t.Error("expected output to contain 'Cluster ID'")
			}

			if cli.DoesNotSee("Port") {
				t.Error("expected output to contain 'Port'")
			}
		})

		t.Run("show config at path", func(t *testing.T) {
			cli := test.NewTestCLI(t, nil)

			directory := t.TempDir()
			configPath := directory + "/config.yml"

			c := config.CLIConfiguration{
				Server: config.CLIServerConfiguration{
					ClusterID: "test-cluster",
					Port:      "9999",
				},
			}

			data, err := yaml.Marshal(c)

			if err != nil {
				t.Fatalf("failed to marshal config: %v", err)
			}

			if err = os.WriteFile(configPath, data, 0644); err != nil {
				t.Fatalf("failed to write config file: %v", err)
			}

			err = cli.Run("config", "show", "--config", configPath)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			if cli.DoesNotSee("Litebase Server Config") {
				t.Error("expected output to contain 'Litebase Server Config'")
			}

			if cli.DoesNotSee("Cluster ID") {
				t.Error("expected output to contain 'Cluster ID'")
			}

			if cli.DoesNotSee("Port") {
				t.Error("expected output to contain 'Port'")
			}
		})

		t.Run("show config that doesn't exist", func(t *testing.T) {
			cli := test.NewTestCLI(t, nil)

			directory := t.TempDir()
			configPath := directory + "/config123.yml"

			err := cli.Run("config", "show", "--config", configPath)

			if err.Error() != "the specified config file does not exist" {
				t.Fatalf("expected error message, got %v", err)
			}
		})
	})
}
