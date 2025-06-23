package cmd

import (
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

func NewClusterUserCreateCmd(config *config.Configuration) *cobra.Command {
	return &cobra.Command{
		Use:   "create",
		Short: "Create a new user",
		RunE: func(cmd *cobra.Command, args []string) error {
			components.NewForm(
				[]components.FormField{
					{
						Name:        "username",
						Label:       "Username",
						Placeholder: "Enter a unique username",
						Required:    true,
						Type:        components.TextType,
					}, {
						Name:        "password",
						Label:       "Password",
						Placeholder: "Enter a strong password",
						Required:    true,
						Type:        components.PasswordType,
					},
					{
						Name:  "privileges",
						Label: "Privileges",
						// Placeholder: "Enter a strong password",
						Required: true,
						Type:     components.CheckboxGroupType,
						Options: map[string]string{
							"Create Access Key": "create_access_key",
							"Read Access Key":   "read_access_key",
							"Update Access Key": "update_access_key",
							"Delete Access Key": "delete_access_key",
						},
					},
				}).
				Title("Create a new user").
				SuccessMessage("User created successfully").
				Method("POST").
				Action(config, "/resources/users/create").
				// Handler(func(data any) error {
				// 	return nil
				// }).
				Render()

			return nil
		},
	}
}
