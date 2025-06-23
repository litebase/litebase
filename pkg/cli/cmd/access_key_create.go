package cmd

import (
	"fmt"

	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

func NewAccessKeyCreateCmd(config *config.Configuration) *cobra.Command {
	return &cobra.Command{
		Use:   "create",
		Short: "Create a new access key",
		RunE: func(cmd *cobra.Command, args []string) error {
			res, _, err := api.Post(config, "/resources/access-keys", map[string]any{})

			if err != nil {
				return err
			}

			fmt.Print(
				components.Container(
					components.SuccessAlert(res["message"].(string)),
					components.NewCard(
						components.WithCardTitle("Access Key"),
						components.WithCardRows([]components.CardRow{
							{
								Key:   "access_key_id",
								Value: res["data"].(map[string]any)["access_key_id"].(string),
							},
							{
								Key:   "access_key_secret",
								Value: res["data"].(map[string]any)["access_key_secret"].(string),
							},
						}),
					).View(),
				),
			)

			return nil
		},
	}
}
