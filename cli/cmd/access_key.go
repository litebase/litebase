package cmd

import "github.com/spf13/cobra"

var AccessKeyCmd = &cobra.Command{
	Use:   "access-key",
	Short: "Manage access keys",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
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
	Run: func(cmd *cobra.Command, args []string) {
	},
}

func NewAccessKeyCmd() *cobra.Command {
	AccessKeyCmd.AddCommand(NewAccessKeyListCmd())
	AccessKeyCmd.AddCommand(NewAccessKeyCreateCmd())
	AccessKeyCmd.AddCommand(NewAccessKeyDeleteCmd())
	AccessKeyCmd.AddCommand(AccessKeyUpdateCmd)

	return AccessKeyCmd
}
