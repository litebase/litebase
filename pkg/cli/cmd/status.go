package cmd

import (
	"log"

	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

func NewStatusCmd(c *config.Configuration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show the status of the cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			res, err := api.Get(c, "/status")

			if err != nil {
				return err
			}

			// fmt.Fprint(
			// 	cmd.OutOrStdout(),
			// 	components.Container(
			// 		components.SuccessAlert(res["message"].(string)),
			// 	),
			// )

			log.Println("res", res)

			return nil
		},
	}

	return cmd
}
