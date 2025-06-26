package cmd

import (
	"context"
	"fmt"

	"github.com/litebase/litebase/pkg/cli"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/charmbracelet/fang"
	"github.com/charmbracelet/lipgloss/v2"
	"github.com/spf13/cobra"
)

var (
	accessKeyId     string
	accessKeySecret string
	noInteraction   bool
	profile         string
	url             string
	username        string
	password        string
)

func addCommands(cmd *cobra.Command, c *config.Configuration) {
	cmd.AddCommand(NewAccessKeyCmd(c))
	cmd.AddCommand(NewClusterCmd(c))
	cmd.AddCommand(NewDatabaseCmd(c))
	cmd.AddCommand(NewAuthCmd())
	cmd.AddCommand(NewProfileCmd(c))
	cmd.AddCommand(NewServeCmd())
	cmd.AddCommand(NewSQLCmd(c))
	cmd.AddCommand(NewStatusCmd(c))
	cmd.AddCommand(NewUserCmd(c))
}

func RootCmd(configPath string) (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:               "litebase <command> <subcommand> [flags]",
		Short:             "Litebase CLI",
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
		Long:              `Connect with Litebase from the command line`,
		Example: `
		litebase database create app_db
		litebase database list
		litebase shell
		litebase sql "SELECT * FROM users"
		`,
		RunE: func(cmd *cobra.Command, args []string) error {
			title := lipgloss.NewStyle().Bold(true).
				Margin(0, 0, 1).
				Render("Litebase CLI - v0.0.1")

			listSlice := []map[string]string{
				{
					"key":   "Website",
					"value": "https://litebase.com",
				},
				{
					"key":   "Docs",
					"value": "https://litebase.com/docs",
				},
				{
					"key":   "GitHub",
					"value": "https://github.com/litebase/litebase",
				},
			}

			container := components.Container(
				fmt.Sprintf(
					"%s\n%s\n\n\n%s",
					title,
					"For help type \"litebase help\"",
					components.TabularList(listSlice),
				),
			)

			lipgloss.Fprint(
				cmd.OutOrStdout(),
				container,
			)

			return nil
		},
	}

	cmd.PersistentFlags().StringVarP(&accessKeyId, "access-key-id", "k", "", "Access key ID for authentication")
	cmd.PersistentFlags().StringVarP(&accessKeySecret, "access-key-secret", "s", "", "Access key secret for authentication")
	cmd.PersistentFlags().StringVarP(&configPath, "config", "c", configPath, "Path to a configuration file")
	cmd.PersistentFlags().StringVarP(&profile, "profile", "p", "", "The profile to use during this session")
	cmd.PersistentFlags().StringVar(&url, "url", "", "Cluster url")
	cmd.PersistentFlags().StringVar(&username, "username", "", "Username for basic authentication")
	cmd.PersistentFlags().StringVar(&password, "password", "", "Password for basic authentication")

	cmd.PersistentFlags().BoolVarP(&noInteraction, "no-interaction", "n", false, "Run without user interaction")

	configuration, err := config.NewConfiguration(configPath)

	if err != nil {
		return nil, err
	}

	addCommands(cmd, configuration)

	cmd.PersistentPreRunE = preRun(configuration)

	return cmd, nil
}

func NewRoot(version string) error {
	cmd, err := RootCmd("")

	if err != nil {
		return err
	}

	return fang.Execute(
		context.Background(),
		cmd,
		fang.WithTheme(cli.ColorScheme()),
		fang.WithVersion(version),
	)
}

func preRun(c *config.Configuration) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		if accessKeyId != "" {
			c.SetAccessKeyId(accessKeyId)
		}

		if accessKeySecret != "" {
			c.SetAccessKeySecret(accessKeySecret)
		}

		c.SetInteractive(!noInteraction)

		if password != "" {
			c.SetPassword(password)
		}

		if url != "" {
			c.SetUrl(url)
		}

		if username != "" {
			c.SetUsername(username)
		}

		return nil
	}
}
