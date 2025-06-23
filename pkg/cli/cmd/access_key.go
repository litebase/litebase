package cmd

import "github.com/spf13/cobra"

var AccessKeyCmd = &cobra.Command{
	Use:   "access-key",
	Short: "Manage access keys",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		err := cmd.Help()

		if err != nil {
			return err
		}

		return nil
	},
}

var AccessKeyUpdateCmd = &cobra.Command{
	Use: "update <id>",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return cobra.MinimumNArgs(1)(cmd, args)
		}

		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		return nil
	},
}

func NewAccessKeyCmd() *cobra.Command {
	AccessKeyCmd.AddCommand(NewAccessKeyListCmd())
	AccessKeyCmd.AddCommand(NewAccessKeyCreateCmd())
	AccessKeyCmd.AddCommand(NewAccessKeyDeleteCmd())
	AccessKeyCmd.AddCommand(AccessKeyUpdateCmd)

	return AccessKeyCmd
}
