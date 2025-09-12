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
	var app *server.App
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
				if err := os.Setenv("LITEBASE_DATA_PATH", dataPath); err != nil {
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

			// TODO: Validate the configuration to ensure all required fields are set

			serverConfig := config.NewConfig()

			server.NewServer(serverConfig).
				OnStarted(func() {
					rows := []components.CardRow{
						{
							Key:   "Port",
							Value: serverConfig.Port,
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
				}).
				Start(func(s *http.ServeMux) {
					app = server.NewApp(serverConfig, s)

					app.Run()

					<-app.Cluster.Node().Start()
				}, func() {
					if app == nil {
						return
					}

					err := app.Cluster.Node().Shutdown()

					if err != nil {
						log.Fatalf("Node shutdown: %v", err)
					}
				})

			return nil
		},
	}

	// Configuration (setup before command runs)
	cobra.OnInitialize(func() {
		if cmd.Flags().Lookup("debug") != nil {
			startConfig.Debug = cmd.Flags().Lookup("debug").Value.String() == "true"
		}

		if err := startConfig.Load(); err != nil {
			slog.Error("failed to load start config", "error", err)
		}
	})

	// Flags
	cmd.Flags().StringVar(&startConfig.ConfigPath, "config", "", "Path to the configuration file")
	cmd.Flags().BoolVarP(&startConfig.Debug, "debug", "d", false, "Run the server in debug mode")
	cmd.Flags().StringVar(&startConfig.Key, "key", "", "The key to use for server encryption")
	cmd.Flags().StringVar(&startConfig.Port, "port", "8080", "The port to run the server on")
	cmd.Flags().StringVar(&startConfig.StoragePath, "storage-path", "", "The path to the data directory")
	cmd.Flags().StringVar(&startConfig.StorageNetworkPath, "storage-network-path", "", "The path to use for network storage")
	cmd.Flags().StringVar(&startConfig.StorageTmpPath, "storage-tmp-path", "", "The path to use for temporary files")
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
	if debug, err := cmd.Flags().GetBool("debug"); err == nil {
		config.Debug = debug
	}

	if port, err := cmd.Flags().GetString("port"); err == nil {
		config.Port = port
	}

	if storagePath, err := cmd.Flags().GetString("storage-path"); err == nil {
		config.StoragePath = storagePath
	}

	if storageNetworkPath, err := cmd.Flags().GetString("storage-network-path"); err == nil {
		config.StorageNetworkPath = storageNetworkPath
	}

	if storageTmpPath, err := cmd.Flags().GetString("storage-tmp-path"); err == nil {
		config.StorageTmpPath = storageTmpPath
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
