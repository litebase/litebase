package cmd

import (
	"fmt"
	"litebase/cli/api"
	"litebase/cli/components"

	"github.com/spf13/cobra"
)

func NewAccessKeyCreateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "create --cluster <name>",
		Short: "Create a new access key",
		Run: func(cmd *cobra.Command, args []string) {
			res, _, err := api.Post("/access-keys", map[string]interface{}{})

			if err != nil {
				fmt.Print(components.Container(components.ErrorAlert(err.Error())))
				return
			}

			fmt.Print(
				components.Container(
					components.SuccessAlert(res["message"].(string)),
					components.NewCard(
						components.WithCardTitle("Access Key"),
						components.WithCardRows([]components.CardRow{
							{
								Key:   "access_key_id",
								Value: res["data"].(map[string]interface{})["access_key_id"].(string),
							},
							{
								Key:   "access_key_secret",
								Value: res["data"].(map[string]interface{})["access_key_secret"].(string),
							},
						}),
					).View(),
				),
			)
		},
	}
}
