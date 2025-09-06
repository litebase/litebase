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
	interactive     bool
	profile         string
	token           string
	url             string
	username        string
	password        string
)

func addCommands(cmd *cobra.Command, c *config.CLIConfiguration) {
	cmd.AddCommand(NewAccessKeyCmd(c))
	cmd.AddCommand(NewConfigCmd(c))
	cmd.AddCommand(NewDatabaseCmd(c))
	cmd.AddCommand(NewDatabaseCmd(c))
	cmd.AddCommand(NewProfileCmd(c))
	cmd.AddCommand(NewStartCmd())
	cmd.AddCommand(NewStatusCmd(c))
	cmd.AddCommand(NewTokenCmd(c))
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
		litebase database query app_db/main "SELECT * FROM users"
		`,
		RunE: func(cmd *cobra.Command, args []string) error {
			title := lipgloss.NewStyle().Bold(true).
				Margin(0, 0, 1).
				Render(fmt.Sprintf("Litebase CLI - %s", cmd.Version))

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

			_, err := lipgloss.Fprint(
				cmd.OutOrStdout(),
				container,
			)

			return err
		},
	}

	// Create configuration with the default path first
	c, err := config.NewConfiguration(configPath, false)

	if err != nil {
		return nil, err
	}

	cmd.PersistentFlags().StringVarP(&accessKeyId, "access-key-id", "k", "", "Access key ID for authentication")
	cmd.PersistentFlags().StringVarP(&accessKeySecret, "access-key-secret", "s", "", "Access key secret for authentication")
	cmd.PersistentFlags().StringVarP(&configPath, "config", "c", configPath, "Path to a configuration file")
	cmd.PersistentFlags().StringVarP(&profile, "profile", "p", "", "The profile to use during this session")
	cmd.PersistentFlags().StringVar(&url, "url", "", "Cluster url")
	cmd.PersistentFlags().StringVar(&token, "token", "", "Cluster token")
	cmd.PersistentFlags().StringVar(&username, "username", "", "Username for basic authentication")
	cmd.PersistentFlags().StringVar(&password, "password", "", "Password for basic authentication")

	cmd.PersistentFlags().BoolVarP(&interactive, "interactive", "i", true, "Run with user interaction")

	// Add commands with the configuration
	addCommands(cmd, c)

	cmd.PersistentPreRunE = preRun(c)

	return cmd, nil
}

func NewRoot(version string) {
	cmd, _ := RootCmd("")

	err := fang.Execute(
		context.Background(),
		cmd,
		fang.WithColorSchemeFunc(cli.ColorScheme),
		fang.WithVersion(version),
	)

	if err != nil {
		return
	}
}

func preRun(c *config.CLIConfiguration) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		cmd.SetErrPrefix("[Litebase]")

		if accessKeyId != "" {
			c.SetAccessKeyId(accessKeyId)
		}

		if accessKeySecret != "" {
			c.SetAccessKeySecret(accessKeySecret)
		}

		c.SetInteractive(interactive)

		if password != "" {
			c.SetPassword(password)
		}

		if token != "" {
			c.SetToken(token)
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
