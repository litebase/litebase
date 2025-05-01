package cmd

import (
	"fmt"
	"strings"

	"github.com/litebase/litebase/server/auth"

	"github.com/litebase/litebase/cli/api"
	"github.com/litebase/litebase/cli/components"

	"github.com/spf13/cobra"
)

type UserListResponse struct {
	Data []auth.User `json:"data"`
}

func NewClusterUserListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List users",
		Run: func(cmd *cobra.Command, args []string) {
			res, err := api.Get("/users")

			if err != nil {
				fmt.Print(components.Container(components.ErrorAlert(err.Error())))
				return
			}

			columns := []string{
				"Username",
				"Privileges",
				"Created At",
				"Updated At",
			}

			var users []auth.User

			for _, user := range res["data"].([]interface{}) {
				privileges := []string{}

				for _, priv := range user.(map[string]interface{})["privileges"].([]interface{}) {
					privileges = append(privileges, priv.(string))
				}

				users = append(users, auth.User{
					Username:   user.(map[string]interface{})["username"].(string),
					Privileges: privileges,
					CreatedAt:  user.(map[string]interface{})["created_at"].(string),
					UpdatedAt:  user.(map[string]interface{})["updated_at"].(string),
				})
			}

			rows := [][]string{}

			for _, user := range users {
				priv := user.Privileges

				rows = append(rows, []string{
					user.Username,
					strings.Join(priv, ", "),
					user.CreatedAt,
					user.UpdatedAt,
				})
			}

			components.NewTable(columns, rows).Render()
		},
	}
}
