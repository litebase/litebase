package cmd

import (
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

func NewProfileCreateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "create",
		Short: "Create a new profile",
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			components.NewForm(
				[]components.FormField{
					{
						Name:        "name",
						Label:       "Name",
						Placeholder: "Enter a unique name",
						Required:    true,
						Type:        components.TextType,
					},
					{
						Name:        "cluster",
						Label:       "Cluster",
						Placeholder: "Enter a cluster url",
						Required:    true,
						Type:        components.TextType,
					},
					{
						Name:     "type",
						Label:    "Select the type of profile",
						Type:     components.RadioGroupType,
						Required: true,
						Options: map[string]string{
							"Access Key": config.ProfileTypeAccessKey,
							"Basic Auth": config.ProfileTypeBasicAuth,
						},
					},
					{
						Name:        "username",
						Label:       "Username",
						Placeholder: "Enter a username",
						Required:    true,
						Type:        components.TextType,
						Conditions: []components.Condition{
							{
								FieldName: "type",
								Operator:  "=",
								Value:     config.ProfileTypeBasicAuth,
							},
						},
					},
					{
						Name:        "password",
						Label:       "Password",
						Placeholder: "Enter a password",
						Required:    true,
						Type:        components.PasswordType,
						Conditions: []components.Condition{
							{
								FieldName: "type",
								Operator:  "=",
								Value:     config.ProfileTypeBasicAuth,
							},
						},
					},
					{
						Name:        "accessKeyId",
						Label:       "Access Key Id",
						Placeholder: "Enter an access key id",
						Required:    true,
						Type:        components.TextType,
						Conditions: []components.Condition{
							{
								FieldName: "type",
								Operator:  "=",
								Value:     config.ProfileTypeAccessKey,
							},
						},
					},
					{
						Name:        "accessKeySecret",
						Label:       "Access Key Secret",
						Placeholder: "Enter a password",
						Required:    true,
						Type:        components.PasswordType,
						Conditions: []components.Condition{
							{
								FieldName: "type",
								Operator:  "=",
								Value:     config.ProfileTypeAccessKey,
							},
						},
					},
				},
			).
				Handler(func(f *components.Form, requestData interface{}, responseData interface{}, err error) error {
					f.SuccessMessage("Profile stored successfully")

					profiles := config.GetProfiles()

					if profiles == nil {
						profiles = []config.Profile{}
					}

					return config.AddProfile(config.Profile{
						Name:    requestData.(map[string]interface{})["name"].(string),
						Cluster: requestData.(map[string]interface{})["cluster"].(string),
						Type:    config.ProfileType(requestData.(map[string]interface{})["type"].(string)),
						Credentials: config.ProfileCredentials{
							Username:        requestData.(map[string]interface{})["username"].(string),
							Password:        requestData.(map[string]interface{})["password"].(string),
							AccessKeyId:     requestData.(map[string]interface{})["accessKeyId"].(string),
							AccessKeySecret: requestData.(map[string]interface{})["accessKeySecret"].(string),
						},
					})
				}).
				Render()
		},
	}
}
