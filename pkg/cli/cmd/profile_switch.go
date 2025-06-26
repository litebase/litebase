package cmd

import (
	"fmt"

	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

func NewProfileSwitchCmd(c *config.Configuration) *cobra.Command {
	return &cobra.Command{
		Use:   "switch <name>",
		Short: "Switch to a different profile",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var profileName string

			if !c.GetInteractive() && len(args) == 1 || c.GetInteractive() && len(args) == 1 {
				profileName = args[0]
			} else {
				profiles := c.GetProfiles()

				columns := []string{"Name", "Cluster"}

				rows := [][]string{}

				for _, profile := range profiles {
					rows = append(rows, []string{profile.Name, profile.Cluster})
				}

				lipgloss.Fprint(
					cmd.OutOrStdout(),
					components.NewTable(columns, rows).
						SetHandler(func(row []string) {
							profileName = row[0]
						}).
						Render(c.GetInteractive()),
				)
			}

			if profileName == "" {
				return fmt.Errorf("profile name is required")
			}

			err := c.SwitchProfile(profileName)

			if err != nil {
				return fmt.Errorf("failed to switch profile: %w", err)
			}

			lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.SuccessAlert(fmt.Sprintf("Profile switched successfully to '%s'", profileName)),
				),
			)
			return nil
		},
	}
}
