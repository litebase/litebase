package cmd

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"

	"github.com/charmbracelet/huh"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
	"go.yaml.in/yaml/v4"
)

// generateEncryptionKey creates a cryptographically secure random encryption key
func generateEncryptionKey() (string, error) {
	length := 64
	randomBytes := make([]byte, (length+1)/2) // Ensure enough bytes for the desired length

	_, err := rand.Read(randomBytes)

	if err != nil {
		log.Fatal(err)
	}

	return hex.EncodeToString(randomBytes)[:length], nil
}

func NewConfigInitCmd(c *config.CLIConfiguration) *cobra.Command {
	newConfig := &config.CLIConfiguration{
		APIVersion: config.CLIConfigurationVersion,
	}

	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize Litebase Server configuration",
		RunE: func(cmd *cobra.Command, args []string) error {
			var confirmed bool
			var path string
			var err error

			// Check if a path was provided in flags
			path, _ = cmd.Flags().GetString("path")

			// If no path was provided, store in the default location for global configuration
			if path == "" {
				path, err = config.DefaultConfigPath()

				if err != nil {
					return fmt.Errorf("failed to get default config path: %w", err)
				}
			}

			// Ensure the directory exists for both custom and default paths
			if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
				return fmt.Errorf("failed to create config directory: %w", err)
			}

			// Check if config already exists - init should only happen once
			if _, err := os.Stat(path); err == nil {
				return fmt.Errorf("configuration file already exists at %s. Configuration can only be initialized once", path)
			} else if !os.IsNotExist(err) {
				return fmt.Errorf("failed to check if config file exists: %w", err)
			}

			// Generate encryption key if not provided via flag
			if newConfig.Server.Key == "" {
				generatedKey, err := generateEncryptionKey()
				if err != nil {
					return fmt.Errorf("failed to generate encryption key: %w", err)
				}
				newConfig.Server.Key = generatedKey
			}

			// Check if we're in non-interactive mode (flags provided)
			if !c.GetInteractive() {
				if newConfig.Server.ClusterID == "" {
					return fmt.Errorf("the cluster-id field is required")
				}

				if newConfig.Server.Port == "" {
					return fmt.Errorf("the port field is required")
				}

				confirmed = true
			} else {
				form := components.NewForm(
					huh.NewGroup(
						huh.NewNote().
							Title("Litebase Server Config").
							Description("Please fill out the following fields to configure your Litebase Server."),
						huh.NewInput().
							Title("Cluster ID").
							Placeholder("Enter the Cluster ID for the server").
							Value(&newConfig.Server.ClusterID).
							CharLimit(255).
							Validate(func(str string) error {
								if str == "" {
									return fmt.Errorf("cluster ID is required")
								}

								return nil
							}),
						huh.NewInput().
							Title("Port").
							Placeholder("Enter the port for the server").
							Value(&newConfig.Server.Port).
							CharLimit(5).
							Validate(func(str string) error {
								if str == "" {
									return fmt.Errorf("port is required")
								}

								if len(str) < 2 || len(str) > 5 {
									return fmt.Errorf("port must be between 2 and 5 characters")
								}

								if _, err := strconv.Atoi(str); err != nil {
									return fmt.Errorf("port must be a number")
								}

								return nil
							}),
					),
					huh.NewGroup(
						huh.NewConfirm().
							Title("Confirm").
							Description("Are you sure you want to save this configuration?").
							Value(&confirmed),
					),
				)

				err = form.Run()

				if err != nil {
					return err
				}
			}

			if !confirmed {
				return nil
			}

			file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)

			if err != nil {
				log.Print("Failed to open config file: ", err)
				return err
			}

			defer func() {
				if err := file.Close(); err != nil {
					slog.Error("failed to close config file", "error", err)
				}
			}()

			encoder := yaml.NewEncoder(file)

			defer func() {
				if err := encoder.Close(); err != nil {
					slog.Error("failed to close config encoder", "error", err)
				}
			}()

			if err := encoder.Encode(newConfig); err != nil {
				return err
			}

			return configShow(cmd, newConfig)
		},
	}

	// Add flags for each field in StartConfig
	cmd.Flags().String("path", "", "Path to the configuration file")
	cmd.Flags().StringVar(&newConfig.Server.ClusterID, "cluster-id", "", "Cluster ID for the server")
	cmd.Flags().StringVar(&newConfig.Server.ConfigPath, "config-path", "", "Path to the configuration file")
	cmd.Flags().BoolVar(&newConfig.Server.Debug, "debug", false, "Enable debug mode")
	cmd.Flags().StringVar(&newConfig.Server.Key, "key", "", "Encryption key (if not provided, one will be generated)")
	cmd.Flags().StringVar(&newConfig.Server.Port, "port", "9876", "Port to run the server on")
	cmd.Flags().StringVar(&newConfig.Server.StoragePath, "storage-path", "", "Path to the storage directory")
	cmd.Flags().StringVar(&newConfig.Server.StorageNetworkPath, "storage-network-path", "", "Path to the network storage directory")
	cmd.Flags().StringVar(&newConfig.Server.StorageTmpPath, "storage-tmp-path", "", "Path to the temporary storage directory")
	cmd.Flags().StringVar(&newConfig.Server.TLSCertPath, "tls-cert-path", "", "Path to the TLS certificate")
	cmd.Flags().StringVar(&newConfig.Server.TLSKeyPath, "tls-key-path", "", "Path to the TLS key")

	hideAuthFlags(cmd)

	return cmd
}
