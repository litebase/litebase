package cmd

import (
	"time"

	"github.com/litebase/litebase/pkg/auth"

	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

type UserListResponse struct {
	Data []auth.User `json:"data"`
}

func NewClusterUserListCmd(config *config.Configuration) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List users",
		RunE: func(cmd *cobra.Command, args []string) error {
			res, err := api.Get(config, "/resources/users")

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

				parsedCreatedAt, err := time.Parse(time.RFC3339, user.(map[string]any)["created_at"].(string))
				if err != nil {
					return err
				}

				parsedUpdatedAt, err := time.Parse(time.RFC3339, user.(map[string]any)["updated_at"].(string))

				if err != nil {
					return err
				}

				users = append(users, auth.User{
					Username:   user.(map[string]any)["username"].(string),
					Statements: statements,
					CreatedAt:  parsedCreatedAt,
					UpdatedAt:  parsedUpdatedAt,
				})
			}

			rows := [][]string{}

			for _, user := range users {
				rows = append(rows, []string{
					user.Username,
					user.CreatedAt.Format(time.RFC3339),
					user.UpdatedAt.Format(time.RFC3339),
				})
			}

			components.NewTable(columns, rows).Render()

			return nil
		},
	}
}
