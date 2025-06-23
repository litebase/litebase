package cmd

import (
	"github.com/litebase/litebase/pkg/auth"

	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"

	"github.com/spf13/cobra"
)

type UserListResponse struct {
	Data []auth.User `json:"data"`
}

func NewClusterUserListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List users",
		RunE: func(cmd *cobra.Command, args []string) error {
			res, err := api.Get("/resources/users")

			if err != nil {
				return err
			}

			columns := []string{
				"Username",
				"Privileges",
				"Created At",
				"Updated At",
			}

			var users []auth.User

			for _, user := range res["data"].([]any) {
				statements := []auth.AccessKeyStatement{}

				for _, priv := range user.(map[string]any)["privileges"].([]any) {
					statements = append(statements, auth.AccessKeyStatement{
						Effect:   "Allow",
						Resource: "*",
						Actions:  []auth.Privilege{priv.(auth.Privilege)},
					})
				}

				users = append(users, auth.User{
					Username:   user.(map[string]any)["username"].(string),
					Statements: statements,
					CreatedAt:  user.(map[string]any)["created_at"].(string),
					UpdatedAt:  user.(map[string]any)["updated_at"].(string),
				})
			}

			rows := [][]string{}

			for _, user := range users {
				rows = append(rows, []string{
					user.Username,
					user.CreatedAt,
					user.UpdatedAt,
				})
			}

			components.NewTable(columns, rows).Render()

			return nil
		},
	}
}
