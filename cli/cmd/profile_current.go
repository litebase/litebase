package cmd

import (
	"fmt"

	"github.com/litebase/litebase/cli/components"
	"github.com/litebase/litebase/cli/config"

	"github.com/spf13/cobra"
)

func NewProfileCurrentCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "current",
		Short: "Return the current profile",
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			profiles, err := config.GetCurrentProfile()

			if err != nil {
				fmt.Println(err)
				return
			}

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
