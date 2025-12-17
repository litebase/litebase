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

	var deploymentMode string
	var storageObjectMode string
	var storageBucket string
	var storageEndpoint string
	var storageRegion string
	var storageAccessKeyId string
	var storageSecretAccessKey string

	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize Litebase Server configuration",
		RunE: func(cmd *cobra.Command, args []string) error {
			var confirmed = true
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
			} else {
				// If path is a directory, append the default config filename
				if fileInfo, err := os.Stat(path); err == nil && fileInfo.IsDir() {
					path = filepath.Join(path, "config.yml")
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

			// Check if we're in non-interactive mode (flags provided OR interactive flag is false)
			nonInteractive := !c.GetInteractive() || (newConfig.Server.ClusterID != "" && newConfig.Server.Port != "")

			if nonInteractive {
				// Non-interactive mode: validate required fields
				if newConfig.Server.ClusterID == "" {
					return fmt.Errorf("the cluster-id field is required")
				}

				if newConfig.Server.Port == "" {
					return fmt.Errorf("the port field is required")
				}

				// Handle deployment mode in non-interactive mode
				if deploymentMode != "" {
					if deploymentMode != "single" && deploymentMode != "cluster" {
						return fmt.Errorf("deployment-mode must be either 'single' or 'cluster'")
					}

					if deploymentMode == "single" {
						// Clear network storage path for single-node deployments
						newConfig.Server.StorageNetworkPath = ""
					} else if deploymentMode == "cluster" && newConfig.Server.StorageNetworkPath == "" {
						return fmt.Errorf("--storage-network-path is required when deployment-mode is 'cluster'")
					}
				}

				// Handle storage configuration in non-interactive mode
				if storageObjectMode != "" {
					// Validate storage mode value
					if storageObjectMode != "local" && storageObjectMode != "object" {
						return fmt.Errorf("storage-object-mode must be either 'local' or 'object'")
					}

					// Set storage mode to config
					newConfig.Server.StorageObjectMode = storageObjectMode

					// When object storage is configured, validate required credentials
					if storageObjectMode == "object" {
						if newConfig.Server.StorageBucket == "" {
							return fmt.Errorf("--storage-bucket is required when storage-object-mode is 'object'")
						}
						if newConfig.Server.StorageEndpoint == "" {
							return fmt.Errorf("--storage-endpoint is required when storage-object-mode is 'object'")
						}
						if newConfig.Server.StorageRegion == "" {
							return fmt.Errorf("--storage-region is required when storage-object-mode is 'object'")
						}
						if newConfig.Server.StorageAccessKeyId == "" {
							return fmt.Errorf("--storage-access-key-id is required when storage-object-mode is 'object'")
						}
						if newConfig.Server.StorageSecretAccessKey == "" {
							return fmt.Errorf("--storage-secret-access-key is required when storage-object-mode is 'object'")
						}
					}
				}

				confirmed = true
			} else {
				// Interactive mode: show form
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
						huh.NewSelect[string]().
							Title("Deployment Mode").
							Description("Will this server run as a single node or part of a cluster?").
							Options(
								huh.NewOption("Single Node (Local storage)", "single"),
								huh.NewOption("Cluster (Shared network storage)", "cluster"),
							).
							Value(&deploymentMode).
							Validate(func(str string) error {
								if str == "" {
									return fmt.Errorf("deployment mode is required")
								}

								return nil
							}),
					),
					huh.NewGroup(
						huh.NewInput().
							Title("Local Storage Path").
							Description("Path to local storage (e.g., ./data)").
							Placeholder("./data").
							Value(&newConfig.Server.StorageLocalPath).
							Validate(func(str string) error {
								if str == "" {
									return fmt.Errorf("local storage path is required for cluster mode")
								}

								return nil
							}),
					),
					huh.NewGroup(
						huh.NewInput().
							Title("Network Storage Path").
							Description("Path to shared network storage (e.g., /mnt/data)").
							Placeholder("/mnt/data").
							Value(&newConfig.Server.StorageNetworkPath).
							Validate(func(str string) error {
								if str == "" {
									return fmt.Errorf("network storage path is required for cluster mode")
								}
								return nil
							}),
					).WithHideFunc(func() bool {
						return deploymentMode == "single"
					}),
					huh.NewGroup(
						huh.NewSelect[string]().
							Title("Object Storage Type").
							Description("Where will persistent data be stored?").
							Options(
								huh.NewOption("Local Object Storage (for testing/development)", "local"),
								huh.NewOption("S3-Compatible Storage (for production)", "object"),
							).
							Value(&storageObjectMode).
							Validate(func(str string) error {
								if str == "" {
									return fmt.Errorf("storage object mode is required")
								}
								return nil
							}),
					),
					huh.NewGroup(
						huh.NewInput().
							Title("Storage Bucket").
							Description("S3 bucket name").
							Placeholder("my-litebase-bucket").
							Value(&newConfig.Server.StorageBucket).
							Validate(func(str string) error {
								if storageObjectMode == "object" && str == "" {
									return fmt.Errorf("bucket name is required for S3 storage")
								}

								return nil
							}),
						huh.NewInput().
							Title("Storage Endpoint").
							Description("S3 endpoint URL (e.g., s3.amazonaws.com)").
							Placeholder("s3.amazonaws.com").
							Value(&newConfig.Server.StorageEndpoint).
							Validate(func(str string) error {
								if storageObjectMode == "object" && str == "" {
									return fmt.Errorf("storage endpoint is required for S3 storage")
								}

								return nil
							}),
						huh.NewInput().
							Title("Storage Region").
							Description("S3 region (e.g., us-east-1)").
							Placeholder("us-east-1").
							Value(&newConfig.Server.StorageRegion).
							Validate(func(str string) error {
								if storageObjectMode == "object" && str == "" {
									return fmt.Errorf("storage region is required for S3 storage")
								}

								return nil
							}),
						huh.NewInput().
							Title("Storage Access Key ID").
							Description("S3 Access Key ID").
							Placeholder("Enter S3 Access Key ID").
							Value(&newConfig.Server.StorageAccessKeyId).
							Validate(func(str string) error {
								if storageObjectMode == "object" && str == "" {
									return fmt.Errorf("access key ID is required for S3 storage")
								}

								return nil
							}),
						huh.NewInput().
							Title("Storage Secret Access Key").
							Description("S3 Secret Access Key").
							Placeholder("Enter S3 Secret Access Key").
							Value(&newConfig.Server.StorageSecretAccessKey).
							EchoMode(huh.EchoModePassword).
							Validate(func(str string) error {
								if storageObjectMode == "object" && str == "" {
									return fmt.Errorf("secret access key is required for S3 storage")
								}

								return nil
							}),
					).WithHideFunc(func() bool {
						return storageObjectMode != "object"
					}),
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

				// Handle deployment mode specific configuration
				switch deploymentMode {
				case "single":
					newConfig.Server.StorageNetworkPath = ""
					fmt.Println("\n✓ Single-node mode: Network storage path will not be configured.")
					fmt.Println("  The server will use local storage only. You can change this later if needed.")
				case "cluster":
					// Ask for cluster-specific configuration
					clusterForm := components.NewForm()

					err = clusterForm.Run()
					if err != nil {
						return err
					}

					fmt.Println("\n✓ Cluster mode: Network storage path configured.")
					fmt.Println("  Ensure this path is accessible from all cluster nodes.")
				}

				// Set storage configuration
				newConfig.Server.StorageObjectMode = storageObjectMode
				if storageObjectMode == "object" {
					newConfig.Server.StorageBucket = storageBucket
					newConfig.Server.StorageEndpoint = storageEndpoint
					newConfig.Server.StorageRegion = storageRegion
					newConfig.Server.StorageAccessKeyId = storageAccessKeyId
					newConfig.Server.StorageSecretAccessKey = storageSecretAccessKey
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
	cmd.Flags().StringVar(&deploymentMode, "deployment-mode", "", "Deployment mode: 'single' for single-node or 'cluster' for multi-node (you can change this later)")
	cmd.Flags().StringVar(&newConfig.Server.ClusterID, "cluster-id", "", "Cluster ID for the server")
	cmd.Flags().StringVar(&newConfig.Server.ConfigPath, "config-path", "", "Path to the configuration file")
	cmd.Flags().BoolVar(&newConfig.Server.Debug, "debug", false, "Enable debug mode")
	cmd.Flags().StringVar(&newConfig.Server.Key, "key", "", "Encryption key (if not provided, one will be generated)")
	cmd.Flags().StringVar(&newConfig.Server.Port, "port", "9876", "Port to run the server on")
	cmd.Flags().StringVar(&newConfig.Server.StorageLocalPath, "storage-path", "", "Path to the storage directory")
	cmd.Flags().StringVar(&newConfig.Server.StorageNetworkPath, "storage-network-path", "", "Path to the network storage directory (leave empty for single-node)")
	cmd.Flags().StringVar(&newConfig.Server.StorageTmpPath, "storage-tmp-path", "", "Path to the temporary storage directory")
	cmd.Flags().StringVar(&newConfig.Server.TLSCertPath, "tls-cert-path", "", "Path to the TLS certificate")
	cmd.Flags().StringVar(&newConfig.Server.TLSKeyPath, "tls-key-path", "", "Path to the TLS key")
	cmd.Flags().StringVar(&storageObjectMode, "storage-object-mode", "local", "Storage object mode: 'local' for testing or 'object' for S3-compatible storage")
	cmd.Flags().StringVar(&newConfig.Server.StorageBucket, "storage-bucket", "", "S3 bucket name (required when storage-object-mode is 'object')")
	cmd.Flags().StringVar(&newConfig.Server.StorageEndpoint, "storage-endpoint", "", "S3 endpoint URL (required when storage-object-mode is 'object')")
	cmd.Flags().StringVar(&newConfig.Server.StorageRegion, "storage-region", "", "S3 region (required when storage-object-mode is 'object')")
	cmd.Flags().StringVar(&newConfig.Server.StorageAccessKeyId, "storage-access-key-id", "", "S3 access key ID (required when storage-object-mode is 'object')")
	cmd.Flags().StringVar(&newConfig.Server.StorageSecretAccessKey, "storage-secret-access-key", "", "S3 secret access key (required when storage-object-mode is 'object')")

	hideAuthFlags(cmd)

	return cmd
}
