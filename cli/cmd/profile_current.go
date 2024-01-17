package cmd

import (
	"fmt"
	"litebasedb/cli/components"
	"litebasedb/cli/config"

	"github.com/spf13/cobra"
)

func NewProfileCurrentCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "current",
		Short: "Return the current profile",
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			profiles := config.GetCurrentProfile()

			fmt.Print(
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
					).View(),
				),
			)
		},
	}
}
