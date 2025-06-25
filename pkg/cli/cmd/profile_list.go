package cmd

import (
	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

func NewProfileListCmd(c *config.Configuration) *cobra.Command {
	return NewCommand("list", "List all profiles").
		WithArgs(cobra.MinimumNArgs(0)).
		WithRunE(func(cmd *cobra.Command, args []string) error {
			profiles := c.GetProfiles()

			columns := []string{"Name", "Cluster"}

			rows := [][]string{}

			for _, profile := range profiles {
				rows = append(rows, []string{profile.Name, profile.Cluster})
			}

			components.NewTable(columns, rows).Render(c.GetInteractive())

			lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.NewTable(columns, rows).Render(c.GetInteractive()),
				),
			)

			return nil
		}).Build()
}
