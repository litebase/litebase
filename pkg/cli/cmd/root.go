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

var confiPath string
var profile string
var url string
var username string
var password string

func addCommands(cmd *cobra.Command) {
	cmd.AddCommand(VersionCmd)
	cmd.AddCommand(NewAccessKeyCmd())
	cmd.AddCommand(NewClusterCmd())
	cmd.AddCommand(NewDatabaseCmd())
	cmd.AddCommand(NewInitCmd())
	cmd.AddCommand(LoginCmd)
	cmd.AddCommand(LogoutCmd)
	cmd.AddCommand(NewProfileCmd())
	cmd.AddCommand(NewServeCmd())
	cmd.AddCommand(NewSQLCmd())
	cmd.AddCommand(NewUserCmd())
}

func NewRoot() error {
	cmd := &cobra.Command{
		Use:               "litebase <command> <subcommand> [flags]",
		Short:             "Litebase CLI",
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
		Long:              `Connect with Litebase from the command line`,
		Run: func(cmd *cobra.Command, args []string) {
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

			fmt.Println(container)
		},
	}

	addCommands(cmd)

	cmd.PersistentFlags().StringVar(&confiPath, "config", "$HOME/.litebase/config", "Path to a configuration file")
	cmd.PersistentFlags().StringVar(&profile, "profile", "", "The profile to use during this session")
	cmd.PersistentFlags().StringVar(&url, "url", "", "Cluster url")
	cmd.PersistentFlags().StringVar(&username, "username", "", "Username")
	cmd.PersistentFlags().StringVar(&password, "password", "", "Password")

	err := config.Init(confiPath)

	if err != nil {
		return err
	}

	cmd.PersistentPreRunE = preRun()

	return fang.Execute(
		context.Background(),
		cmd,
		fang.WithTheme(cli.ColorScheme()),
	)
}

func preRun() func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		if url != "" {
			config.SetUrl(url)
		}

		if username != "" {
			config.SetUsername(username)
		}

		if password != "" {
			config.SetPassword(password)
		}

		return nil
	}
}
