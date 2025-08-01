package cmd

import (
	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

func NewProfileCurrentCmd(c *config.Configuration) *cobra.Command {
	return &cobra.Command{
		Use:   "current",
		Short: "Get the current profile",
		Args:  cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			profiles, err := c.GetCurrentProfile()

			if err != nil {
				return err
			}

			lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.NewCard(
						components.WithCardTitle("Current Profile"),
						components.WithCardRows([]components.CardRow{
							{
								Key:   "Name",
								Value: profiles.Name,
							},
							{
								Key:   "Cluster",
								Value: profiles.Cluster,
							},
						}),
					).Render(),
				),
			)

			return nil
		},
	}
}
