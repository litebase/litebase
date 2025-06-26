package cmd

import (
	"errors"
	"fmt"

	"github.com/charmbracelet/huh"
	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

func NewProfileCreateCmd(c *config.Configuration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a new profile",
		RunE: func(cmd *cobra.Command, args []string) error {
			profiles := c.GetProfiles()

			profile := config.Profile{}

			profile.Name, _ = cmd.Flags().GetString("profile-name")
			profile.Cluster, _ = cmd.Flags().GetString("profile-cluster")
			profile.Type, _ = cmd.Flags().GetString("profile-type")
			profile.Credentials.Username, _ = cmd.Flags().GetString("profile-username")
			profile.Credentials.Password, _ = cmd.Flags().GetString("profile-password")
			profile.Credentials.AccessKeyID, _ = cmd.Flags().GetString("profile-access-key-id")
			profile.Credentials.AccessKeySecret, _ = cmd.Flags().GetString("profile-access-key-secret")

			if c.GetInteractive() {
				form := components.NewForm(
					huh.NewGroup(
						huh.NewInput().
							Title("Name").
							Placeholder("Enter a unique name").
							Validate(func(str string) error {
								if str == "" {
									return errors.New("name cannot be empty")
								}

								for _, p := range profiles {
									if p.Name == str {
										return errors.New("profile with this name already exists")
									}
								}

								return nil
							}).
							CharLimit(100).
							Value(&profile.Name),
						huh.NewInput().
							Title("Cluster").
							Placeholder("Enter a cluster url").
							Validate(func(str string) error {
								if str == "" {
									return errors.New("cluster url cannot be empty")
								}
								return nil
							}).
							Value(&profile.Cluster),
						huh.NewSelect[string]().
							Title("Type").
							Options(
								huh.NewOption(string(config.ProfileTypeAccessKey), string(config.ProfileTypeAccessKey)),
								huh.NewOption(string(config.ProfileTypeBasicAuth), string(config.ProfileTypeBasicAuth)),
							).
							Validate(func(str string) error {
								if str == "" {
									return errors.New("type cannot be empty")
								}

								if str != string(config.ProfileTypeAccessKey) && str != string(config.ProfileTypeBasicAuth) {
									return errors.New("invalid type, must be either 'Access Key' or 'Basic Auth'")
								}

								return nil
							}).
							Value(&profile.Type),
					),
					huh.NewGroup(
						huh.NewInput().
							Title("Username").
							Placeholder("Enter a username").
							Validate(func(str string) error {
								if profile.Type == string(config.ProfileTypeBasicAuth) && str == "" {
									return errors.New("username cannot be empty for Basic Auth")
								}
								return nil
							}).
							Value(&profile.Credentials.Username),
						huh.NewInput().
							Title("Password").
							Placeholder("Enter a password").
							Validate(func(str string) error {
								if profile.Type == string(config.ProfileTypeBasicAuth) && str == "" {
									return errors.New("password cannot be empty for Basic Auth")
								}

								return nil
							}).
							Value(&profile.Credentials.Password),
					).WithHideFunc(func() bool {
						return profile.Type != string(config.ProfileTypeBasicAuth)
					}),
					huh.NewGroup(
						huh.NewInput().
							Title("Access Key Id").
							Placeholder("Enter an access key id").
							Validate(func(str string) error {
								if profile.Type == string(config.ProfileTypeAccessKey) && str == "" {
									return errors.New("access key id cannot be empty for Access Key")
								}

								return nil
							}).
							Value(&profile.Credentials.AccessKeyID),
						huh.NewInput().
							Title("Access Key Secret").
							Placeholder("Enter an access key secret").
							Validate(func(str string) error {
								if profile.Type == string(config.ProfileTypeAccessKey) && str == "" {
									return errors.New("access key secret cannot be empty for Access Key")
								}

								return nil
							}).
							Value(&profile.Credentials.AccessKeySecret),
					).WithHideFunc(func() bool {
						return profile.Type != string(config.ProfileTypeAccessKey)
					}),
				)

				err := form.Run()

				if err != nil {
					return err
				}
			} else {
				if profile.Name == "" || profile.Cluster == "" || profile.Type == "" {
					return errors.New("name, cluster and type are required fields")
				}

				for _, p := range profiles {
					if p.Name == profile.Name {
						return errors.New("profile with this name already exists")
					}
				}

				if profile.Type != string(config.ProfileTypeAccessKey) && profile.Type != string(config.ProfileTypeBasicAuth) {
					return fmt.Errorf("invalid profile type, must be either '%s' or '%s', got '%s'", config.ProfileTypeAccessKey, config.ProfileTypeBasicAuth, profile.Type)
				}

				if profile.Type == string(config.ProfileTypeBasicAuth) {
					if profile.Credentials.Username == "" || profile.Credentials.Password == "" {
						return errors.New("username and password are required for Basic Auth type")
					}
				} else if profile.Type == string(config.ProfileTypeAccessKey) {
					if profile.Credentials.AccessKeyID == "" || profile.Credentials.AccessKeySecret == "" {
						return errors.New("access key id and secret are required for Access Key type")
					}
				} else {
					return errors.New("invalid profile type, must be either 'Access Key' or 'Basic Auth'")
				}
			}

			err := c.AddProfile(profile)

			if err != nil {
				return err
			}

			lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.SuccessAlert("Profile stored successfully"),
				),
			)

			return nil
		},
	}

	// Add flags
	cmd.Flags().String("profile-name", "", "Name of the profile (required)")
	cmd.Flags().String("profile-cluster", "", "Cluster URL (required)")
	cmd.Flags().String("profile-type", "", "Type of profile (Access Key or Basic Auth) (required)")
	cmd.Flags().String("profile-username", "", "Username for Basic Auth (required if type is Basic Auth)")
	cmd.Flags().String("profile-password", "", "Password for Basic Auth (required if type is Basic Auth)")
	cmd.Flags().String("profile-access-key-id", "", "Access Key ID (required if type is Access Key)")
	cmd.Flags().String("profile-access-key-secret", "", "Access Key Secret (required if type is Access Key)")

	return cmd
}
