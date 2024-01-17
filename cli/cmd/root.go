package cmd

import (
	"context"
	"fmt"
	"litebasedb/cli/components"
	"litebasedb/cli/config"

	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"
)

var confiPath string
var profile string
var cluster string
var username string
var password string

func addCommands(cmd *cobra.Command) {
	cmd.AddCommand(VersionCmd)
	cmd.AddCommand(NewAccessKeyCmd())
	cmd.AddCommand(NewClusterCmd())
	cmd.AddCommand(NewDatabaseCmd())
	cmd.AddCommand(LoginCmd)
	cmd.AddCommand(LogoutCmd)
	cmd.AddCommand(NewProfileCmd())
	cmd.AddCommand(NewServeCmd())
	cmd.AddCommand(NewSQLCmd())
	cmd.AddCommand(NewUserCmd())
}

func NewRoot() error {

	cmd := &cobra.Command{
		Use:               "litebasedb <command> <subcommand> [flags]",
		Short:             "LitebaseDB CLI",
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
		Long:              `Connect with LitebaseDB from the command line.`,
		Run: func(cmd *cobra.Command, args []string) {
			title := lipgloss.NewStyle().Copy().Bold(true).
				Margin(0, 0, 1).
				Render("LitebaseDB CLI - v0.0.1")

			listSlice := []map[string]string{
				{
					"key":   "Website",
					"value": "https://litebasedb.com",
				},
				{
					"key":   "Docs",
					"value": "https://litebasedb.com/docs",
				},
				{
					"key":   "GitHub",
					"value": "https://github.com/litebasedb/litebasedb",
				},
			}

			container := components.Container(
				fmt.Sprintf(
					"%s\n%s\n\n\n%s",
					title,
					"For help type \"litebasedb help\"",
					components.TabularList(listSlice),
				),
			)

			fmt.Println(container)
		},
	}

	addCommands(cmd)

	cmd.PersistentFlags().StringVar(&confiPath, "config", "$HOME/.litebasedb/config", "Path to a configuration file (default \"$HOME/.litebasedb/config.json\")")
	cmd.PersistentFlags().StringVar(&profile, "profile", "", "The profile to use during this session")
	cmd.PersistentFlags().StringVar(&cluster, "cluster", "", "Cluster")
	cmd.PersistentFlags().StringVar(&username, "username", "", "Username")
	cmd.PersistentFlags().StringVar(&password, "password", "", "Password")

	config.Init(confiPath)

	cmd.SetHelpFunc(NewHelpCmd())

	cmd.PersistentPreRunE = preRun()

	return cmd.ExecuteContext(context.Background())
}

func preRun() func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		if cluster != "" {
			config.SetCluster(cluster)
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
