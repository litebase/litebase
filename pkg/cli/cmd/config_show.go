package cmd

import (
	"errors"
	"os"

	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

func configShow(cmd *cobra.Command, c *config.CLIConfiguration, showKeys bool) error {
	configPath, err := cmd.Flags().GetString("config")

	if err != nil {
		return err
	}

	// If a specific config path is provided, load from that file
	if configPath != "" {
		if _, err := os.Stat(configPath); err != nil {
			if os.IsNotExist(err) {
				return errors.New("the specified config file does not exist")
			}

			return err
		}

		// Load the configuration from the specified path
		c, err = config.NewConfiguration(configPath, false)

		if err != nil {
			return err
		}
	}

	rows := []components.CardRow{
		{
			Key:   "Cluster ID",
			Value: c.Server.ClusterID,
		},
		{
			Key:   "Port",
			Value: c.Server.Port,
		},
	}

	if showKeys && c.Server.EncryptionKey != "" {
		rows = append(rows, components.CardRow{
			Key:   "Encryption Key",
			Value: c.Server.EncryptionKey,
		})
	}

	if showKeys && c.Server.DataEncryptionKey != "" {
		rows = append(rows, components.CardRow{
			Key:   "Data Encryption Key",
			Value: c.Server.DataEncryptionKey,
		})
	}

	// Add more configuration fields if they have values
	if c.Server.Debug {
		rows = append(rows, components.CardRow{
			Key:   "Debug",
			Value: "true",
		})
	}

	if c.Server.StorageLocalPath != "" {
		rows = append(rows, components.CardRow{
			Key:   "Storage Path",
			Value: c.Server.StorageLocalPath,
		})
	}

	if c.Server.StorageNetworkPath != "" {
		rows = append(rows, components.CardRow{
			Key:   "Storage Network Path",
			Value: c.Server.StorageNetworkPath,
		})
	}

	_, err = lipgloss.Fprint(
		cmd.OutOrStdout(),
		components.Container(
			components.NewCard(
				components.WithCardTitle("Litebase Server Config"),
				components.WithCardRows(rows),
			).Render(),
		),
	)

	return err
}

func NewConfigShowCmd(c *config.CLIConfiguration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show",
		Short: "Show Litebase Server configuration",
		RunE: func(cmd *cobra.Command, args []string) error {
			return configShow(cmd, c, false)
		},
	}

	hideAuthFlags(cmd)

	return cmd
}
