package cmd

import (
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"

	"github.com/charmbracelet/lipgloss/v2"
	"github.com/joho/godotenv"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/server"
	"go.yaml.in/yaml/v4"

	"github.com/spf13/cobra"
)

func NewStartCmd() *cobra.Command {
	startConfig := &StartConfig{}

	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start Litebase Server",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Attempt to load configuration from a local configuration file
			if err := startLoadConfiguration(cmd, startConfig); err != nil {
				return fmt.Errorf("failed to load configuration: %w", err)
			}

			// Attempt to load configuration from an environment variables file
			startLoadEnv()

			// Attempt to load configuration values from the CLI flags
			if dataPath := cmd.Flag("storage-path").Value.String(); dataPath != "" {
				if err := os.Setenv("LITEBASE_STORAGE_LOCAL_PATH", dataPath); err != nil {
					panic(err)
				}
			}

			if networkPath := cmd.Flag("storage-network-path").Value.String(); networkPath != "" {
				if err := os.Setenv("LITEBASE_STORAGE_NETWORK_PATH", networkPath); err != nil {
					panic(err)
				}
			}

			if tmpPath := cmd.Flag("storage-tmp-path").Value.String(); tmpPath != "" {
				if err := os.Setenv("LITEBASE_STORAGE_TMP_PATH", tmpPath); err != nil {
					panic(err)
				}
			}

			// Attempt to load configuration from flags
			if err := startLoadFlags(cmd, startConfig); err != nil {
				return fmt.Errorf("failed to load flags: %w", err)
			}

			// Load startConfig into environment variables (only for non-empty values)
			if err := startConfig.Load(); err != nil {
				return fmt.Errorf("failed to load start config: %w", err)
			}

			// Validate storage configuration before starting the server
			if err := validateStorageConfig(startConfig); err != nil {
				return err
			}

			serverConfig := config.NewConfig()
			srv := server.NewServer(serverConfig)

			srv.StartWithPrivateRouting(
				// Public server setup
				func(publicMux *http.ServeMux, app *server.App) {
					app.Run()

					// Start the node
					<-app.Cluster.Node().Start()

					// Display server info after node starts
					rows := []components.CardRow{
						{
							Key:   "Port",
							Value: serverConfig.Port,
						},
						{
							Key:   "Private Port",
							Value: fmt.Sprintf("%d", srv.GetPrivatePort()),
						},
						{
							Key:   "Cluster ID",
							Value: app.Cluster.ID,
						},
						{
							Key:   "Node ID",
							Value: app.Cluster.Node().ID,
						},
					}

					if startConfig.Debug {
						slog.SetLogLoggerLevel(slog.LevelDebug)

						rows = append(rows, components.CardRow{
							Key:   "Debug Mode",
							Value: "Enabled",
						})
					} else {
						slog.SetLogLoggerLevel(slog.LevelInfo)
					}

					if startConfig.TLSCertPath != "" && startConfig.TLSKeyPath != "" {
						rows = append(rows, components.CardRow{
							Key:   "TLS",
							Value: "Enabled",
						})
					}

					_, err := lipgloss.Fprint(
						cmd.OutOrStdout(),
						components.Container(
							components.NewCard(
								components.WithCardTitle("Litebase Server"),
								components.WithCardRows(rows),
							).Render(),
						),
					)

					if err != nil {
						log.Fatalf("Error printing server info: %v", err)
					}
				},
				// Private server setup
				func(privateMux *http.ServeMux, app *server.App) {
					// Private routes are automatically set up by the server
				},
				// Shutdown hook
				func(app *server.App) {
					err := app.Cluster.Node().Shutdown()

					if err != nil {
						log.Fatalf("Node shutdown: %v", err)
					}
				},
			)

			return nil
		},
	}

	// Configuration (setup before command runs)
	cobra.OnInitialize(func() {
		if cmd.Flags().Lookup("debug") != nil {
			startConfig.Debug = cmd.Flags().Lookup("debug").Value.String() == "true"
		}

		// Only load config if it was explicitly set (don't override environment variables)
		// The Load() method will be called later in RunE after flags are parsed
	})

	// Flags
	cmd.Flags().StringVar(&startConfig.ConfigPath, "config", "", "Path to the configuration file")
	cmd.Flags().BoolVarP(&startConfig.Debug, "debug", "d", false, "Run the server in debug mode")
	cmd.Flags().StringVar(&startConfig.Key, "key", "", "The key to use for server encryption")
	cmd.Flags().StringVar(&startConfig.Port, "port", "", "The port to run the server on (defaults to LITEBASE_PORT env var or 8080)")
	cmd.Flags().StringVar(&startConfig.StorageLocalPath, "storage-path", "", "The path to the data directory")
	cmd.Flags().StringVar(&startConfig.StorageNetworkPath, "storage-network-path", "", "The path to use for network storage")
	cmd.Flags().StringVar(&startConfig.StorageTmpPath, "storage-tmp-path", "", "The path to use for temporary files")
	cmd.Flags().StringVar(&startConfig.StorageObjectMode, "storage-object-mode", "", "Storage object mode: 'local' for testing or 'object' for S3-compatible storage")
	cmd.Flags().StringVar(&startConfig.StorageBucket, "storage-bucket", "", "S3 bucket name (required when storage-object-mode is 'object')")
	cmd.Flags().StringVar(&startConfig.StorageEndpoint, "storage-endpoint", "", "S3 endpoint URL (required when storage-object-mode is 'object')")
	cmd.Flags().StringVar(&startConfig.StorageRegion, "storage-region", "", "S3 region (required when storage-object-mode is 'object')")
	cmd.Flags().StringVar(&startConfig.StorageAccessKeyId, "storage-access-key-id", "", "S3 access key ID (required when storage-object-mode is 'object')")
	cmd.Flags().StringVar(&startConfig.StorageSecretAccessKey, "storage-secret-access-key", "", "S3 secret access key (required when storage-object-mode is 'object')")
	cmd.Flags().StringVar(&startConfig.TLSCertPath, "tls-cert-path", "", "The path to the TLS certificate")
	cmd.Flags().StringVar(&startConfig.TLSKeyPath, "tls-key-path", "", "The path to the TLS key")

	hideAuthFlags(cmd)

	return cmd
}

// Attempt to load a Litebase configuration from the default locations.
func startLoadConfiguration(cmd *cobra.Command, config *StartConfig) error {
	// TODO: Will this load the global configuration file if it exists?
	var configPath string
	localPath := "./.litebase/config.yml"

	// Attempt to load the configuration file from the provided path
	if cp := cmd.Flag("config").Value.String(); cp != "" {
		configPath = cp
	} else if _, err := os.Stat(localPath); err == nil {
		configPath = localPath
	}

	if configPath == "" {
		// No configuration file found; proceed with defaults or environment variables
		return nil
	}

	file, err := os.Open(configPath)

	if err != nil {
		return fmt.Errorf("failed to open config file: %w", err)
	}

	defer func() {
		if err := file.Close(); err != nil {
			slog.Error("failed to close config file", "error", err)
		}
	}()

	decoder := yaml.NewDecoder(file)

	if err := decoder.Decode(config); err != nil {
		return fmt.Errorf("failed to decode config file: %w", err)
	}

	if err := config.Load(); err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	return nil
}

// Attempt to load environment variables from a .env file that is in the current
// working directory.
func startLoadEnv() {
	if err := godotenv.Load(".env"); err != nil {
		slog.Debug("Server .env file not loaded", "error", err)
	}
}

func startLoadFlags(cmd *cobra.Command, config *StartConfig) error {
	if cmd.Flags().Changed("debug") {
		if debug, err := cmd.Flags().GetBool("debug"); err == nil {
			config.Debug = debug
		}
	}

	// Only set port if the flag was explicitly changed from default
	if cmd.Flags().Changed("port") {
		if port, err := cmd.Flags().GetString("port"); err == nil {
			config.Port = port
		}
	}

	if storagePath, err := cmd.Flags().GetString("storage-path"); err == nil {
		config.StorageLocalPath = storagePath
	}

	if storageNetworkPath, err := cmd.Flags().GetString("storage-network-path"); err == nil {
		config.StorageNetworkPath = storageNetworkPath
	}

	if storageTmpPath, err := cmd.Flags().GetString("storage-tmp-path"); err == nil {
		config.StorageTmpPath = storageTmpPath
	}

	if storageObjectMode, err := cmd.Flags().GetString("storage-object-mode"); err == nil {
		config.StorageObjectMode = storageObjectMode
	}

	if storageBucket, err := cmd.Flags().GetString("storage-bucket"); err == nil {
		config.StorageBucket = storageBucket
	}

	if storageEndpoint, err := cmd.Flags().GetString("storage-endpoint"); err == nil {
		config.StorageEndpoint = storageEndpoint
	}

	if storageRegion, err := cmd.Flags().GetString("storage-region"); err == nil {
		config.StorageRegion = storageRegion
	}

	if storageAccessKeyId, err := cmd.Flags().GetString("storage-access-key-id"); err == nil {
		config.StorageAccessKeyId = storageAccessKeyId
	}

	if storageSecretAccessKey, err := cmd.Flags().GetString("storage-secret-access-key"); err == nil {
		config.StorageSecretAccessKey = storageSecretAccessKey
	}

	if tlsCertPath, err := cmd.Flags().GetString("tls-cert-path"); err == nil {
		config.TLSCertPath = tlsCertPath
	}

	if tlsKeyPath, err := cmd.Flags().GetString("tls-key-path"); err == nil {
		config.TLSKeyPath = tlsKeyPath
	}

	if key, err := cmd.Flags().GetString("key"); err == nil {
		config.Key = key
	}

	return nil
}

// validateStorageConfig checks that required storage credentials are present
// when object storage mode is configured.
func validateStorageConfig(config *StartConfig) error {
	// Get the actual storage object mode from environment or config
	storageObjectMode := config.StorageObjectMode

	if storageObjectMode == "" {
		storageObjectMode = os.Getenv("LITEBASE_STORAGE_OBJECT_MODE")
	}

	// Get fake storage mode for testing
	fakeObjectStorage := os.Getenv("LITEBASE_FAKE_OBJECT_STORAGE") == "true"

	// If using object storage mode and not in fake mode, validate credentials
	if storageObjectMode == "object" && !fakeObjectStorage {
		// Check for storage bucket
		if config.StorageBucket == "" && os.Getenv("LITEBASE_STORAGE_BUCKET") == "" {
			return fmt.Errorf("storage bucket is required when using object storage mode. Set via config file, --storage-bucket flag, or LITEBASE_STORAGE_BUCKET environment variable")
		}

		// Check for storage endpoint
		if config.StorageEndpoint == "" && os.Getenv("LITEBASE_STORAGE_ENDPOINT") == "" {
			return fmt.Errorf("storage endpoint is required when using object storage mode. Set via config file, --storage-endpoint flag, or LITEBASE_STORAGE_ENDPOINT environment variable")
		}

		// Check for storage region
		if config.StorageRegion == "" && os.Getenv("LITEBASE_STORAGE_REGION") == "" {
			return fmt.Errorf("storage region is required when using object storage mode. Set via config file, --storage-region flag, or LITEBASE_STORAGE_REGION environment variable")
		}

		// Check for access key ID
		if config.StorageAccessKeyId == "" && os.Getenv("LITEBASE_STORAGE_ACCESS_KEY_ID") == "" {
			return fmt.Errorf("storage access key ID is required when using object storage mode. Set via config file, --storage-access-key-id flag, or LITEBASE_STORAGE_ACCESS_KEY_ID environment variable")
		}

		// Check for secret access key
		if config.StorageSecretAccessKey == "" && os.Getenv("LITEBASE_STORAGE_SECRET_ACCESS_KEY") == "" {
			return fmt.Errorf("storage secret access key is required when using object storage mode. Set via config file, --storage-secret-access-key flag, or LITEBASE_STORAGE_SECRET_ACCESS_KEY environment variable")
		}
	}

	return nil
}
