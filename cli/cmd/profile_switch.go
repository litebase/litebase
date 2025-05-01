package cmd

import (
	"fmt"
	"os"

	"github.com/litebase/litebase/cli/components"
	"github.com/litebase/litebase/cli/config"

	"github.com/spf13/cobra"
)

func NewProfileSwitchCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "switch",
		Short: "Switch to a profile",
		Run: func(cmd *cobra.Command, args []string) {
			profiles := config.GetProfiles()

			columns := []string{"Name", "Cluster"}

			rows := [][]string{}

			for _, profile := range profiles {
				rows = append(rows, []string{profile.Name, profile.Cluster})
			}

			components.NewTable(columns, rows).
				SetHandler(func(row []string) {
					config.SwitchProfile(row[0])

					fmt.Print(
						components.Container(
							components.SuccessAlert(fmt.Sprintf("Profile switched to %s", row[0])),
						),
					)

					os.Exit(0)
				}).Render()
		},
	}

}
