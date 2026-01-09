package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/charmbracelet/huh"
	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

type DatabaseBranchSettingsInput struct {
	BackupsEnabled                  bool                                   `json:"backupsEnabled"`
	BackupInterval                  string                                 `json:"backupInterval"`
	BackupsRetentionDays            int                                    `json:"backupsRetentionDays"`
	DefaultPragmas                  *DatabaseDefaultPragmaSettingsInput    `json:"defaultPragmas,omitempty"`
	ErrorLogsEnabled                bool                                   `json:"errorLogsEnabled"`
	ErrorLogsRetentionDays          int                                    `json:"errorLogsRetentionDays"`
	IncrementalBackupsEnabled       bool                                   `json:"incrementalBackupsEnabled"`
	IncrementalBackupsRetentionDays int                                    `json:"incrementalBackupsRetentionDays"`
	QueryLogsEnabled                bool                                   `json:"queryLogsEnabled"`
	QueryLogsRetentionDays          int                                    `json:"queryLogsRetentionDays"`
}

type DatabaseDefaultPragmaSettingsInput struct {
	ForeignKeys string `json:"foreignKeys"`
}

func NewDatabaseBranchSettingsUpdateCmd(config *config.CLIConfiguration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update <database/branch>",
		Short: "Update database branch settings",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var confirmed bool
			databaseName, branchName, err := splitDatabasePath(args[0])

			if err != nil {
				return fmt.Errorf("invalid database path: %w", err)
			}

			input := DatabaseBranchSettingsInput{}

			// Get flag values
			backupsEnabled, _ := cmd.Flags().GetBool("backups-enabled")
			backupInterval, _ := cmd.Flags().GetString("backup-interval")
			backupsRetentionDays, _ := cmd.Flags().GetInt("backups-retention-days")
			errorLogsEnabled, _ := cmd.Flags().GetBool("error-logs-enabled")
			errorLogsRetentionDays, _ := cmd.Flags().GetInt("error-logs-retention-days")
			incrementalBackupsEnabled, _ := cmd.Flags().GetBool("incremental-backups-enabled")
			incrementalBackupsRetentionDays, _ := cmd.Flags().GetInt("incremental-backups-retention-days")
			queryLogsEnabled, _ := cmd.Flags().GetBool("query-logs-enabled")
			queryLogsRetentionDays, _ := cmd.Flags().GetInt("query-logs-retention-days")
			foreignKeys, _ := cmd.Flags().GetString("foreign-keys")

			// Check if we're in non-interactive mode
			nonInteractive := !config.GetInteractive() || cmd.Flags().Changed("backups-enabled")

			if nonInteractive {
				// Non-interactive mode: use provided flags
				input.BackupsEnabled = backupsEnabled
				input.BackupInterval = backupInterval
				input.BackupsRetentionDays = backupsRetentionDays
				input.ErrorLogsEnabled = errorLogsEnabled
				input.ErrorLogsRetentionDays = errorLogsRetentionDays
				input.IncrementalBackupsEnabled = incrementalBackupsEnabled
				input.IncrementalBackupsRetentionDays = incrementalBackupsRetentionDays
				input.QueryLogsEnabled = queryLogsEnabled
				input.QueryLogsRetentionDays = queryLogsRetentionDays

				if foreignKeys != "" {
					input.DefaultPragmas = &DatabaseDefaultPragmaSettingsInput{
						ForeignKeys: foreignKeys,
					}
				}

				confirmed = true
			} else {
				// Fetch current settings
				res, err := api.Get(config, fmt.Sprintf("/v1/databases/%s/branches/%s/settings", databaseName, branchName))

				if err != nil {
					return err
				}

				data := res["data"].(map[string]any)

				// Pre-fill with existing values
				input.BackupsEnabled = data["backupsEnabled"].(bool)
				input.BackupInterval = data["backupInterval"].(string)
				input.BackupsRetentionDays = int(data["backupsRetentionDays"].(float64))
				input.ErrorLogsEnabled = data["errorLogsEnabled"].(bool)
				input.ErrorLogsRetentionDays = int(data["errorLogsRetentionDays"].(float64))
				input.IncrementalBackupsEnabled = data["incrementalBackupsEnabled"].(bool)
				input.IncrementalBackupsRetentionDays = int(data["incrementalBackupsRetentionDays"].(float64))
				input.QueryLogsEnabled = data["queryLogsEnabled"].(bool)
				input.QueryLogsRetentionDays = int(data["queryLogsRetentionDays"].(float64))

				if data["defaultPragmas"] != nil {
					pragmas := data["defaultPragmas"].(map[string]any)
					input.DefaultPragmas = &DatabaseDefaultPragmaSettingsInput{
						ForeignKeys: pragmas["foreignKeys"].(string),
					}
				}

				// Interactive mode: show form
				// Convert ints to strings for form inputs
				backupsRetentionDaysStr := strconv.Itoa(input.BackupsRetentionDays)
				incrementalBackupsRetentionDaysStr := strconv.Itoa(input.IncrementalBackupsRetentionDays)
				queryLogsRetentionDaysStr := strconv.Itoa(input.QueryLogsRetentionDays)
				errorLogsRetentionDaysStr := strconv.Itoa(input.ErrorLogsRetentionDays)

				form := components.NewForm(
					huh.NewGroup(
						huh.NewNote().
							Title("Update Branch Settings").
							Description("Configure backup and logging settings for this branch."),
					),
					huh.NewGroup(
						huh.NewConfirm().
							Title("Backups Enabled").
							Description("Enable automated backups").
							Value(&input.BackupsEnabled),
						huh.NewInput().
							Title("Backup Interval").
							Description("Backup interval (e.g., 24h, 48h, 168h)").
							Value(&input.BackupInterval).
							Validate(func(str string) error {
								if input.BackupsEnabled && str == "" {
									return errors.New("backup interval is required when backups are enabled")
								}
								return nil
							}),
						huh.NewInput().
							Title("Backups Retention Days").
							Description("How many days to keep backups").
							Value(&backupsRetentionDaysStr).
							Validate(func(str string) error {
								val, err := strconv.Atoi(str)
								if err != nil {
									return errors.New("must be a valid number")
								}
								if input.BackupsEnabled && val < 1 {
									return errors.New("retention days must be at least 1")
								}
								return nil
							}),
					),
					huh.NewGroup(
						huh.NewConfirm().
							Title("Incremental Backups Enabled").
							Description("Enable incremental backups").
							Value(&input.IncrementalBackupsEnabled),
						huh.NewInput().
							Title("Incremental Backups Retention Days").
							Description("How many days to keep incremental backups").
							Value(&incrementalBackupsRetentionDaysStr).
							Validate(func(str string) error {
								val, err := strconv.Atoi(str)
								if err != nil {
									return errors.New("must be a valid number")
								}
								if input.IncrementalBackupsEnabled && val < 1 {
									return errors.New("retention days must be at least 1")
								}
								return nil
							}),
					),
					huh.NewGroup(
						huh.NewConfirm().
							Title("Query Logs Enabled").
							Description("Enable query logging").
							Value(&input.QueryLogsEnabled),
						huh.NewInput().
							Title("Query Logs Retention Days").
							Description("How many days to keep query logs").
							Value(&queryLogsRetentionDaysStr).
							Validate(func(str string) error {
								val, err := strconv.Atoi(str)
								if err != nil {
									return errors.New("must be a valid number")
								}
								if input.QueryLogsEnabled && val < 1 {
									return errors.New("retention days must be at least 1")
								}
								return nil
							}),
					),
					huh.NewGroup(
						huh.NewConfirm().
							Title("Error Logs Enabled").
							Description("Enable error logging").
							Value(&input.ErrorLogsEnabled),
						huh.NewInput().
							Title("Error Logs Retention Days").
							Description("How many days to keep error logs").
							Value(&errorLogsRetentionDaysStr).
							Validate(func(str string) error {
								val, err := strconv.Atoi(str)
								if err != nil {
									return errors.New("must be a valid number")
								}
								if input.ErrorLogsEnabled && val < 1 {
									return errors.New("retention days must be at least 1")
								}
								return nil
							}),
					),
					huh.NewGroup(
						huh.NewConfirm().
							Title("Confirm").
							Description("Are you sure you want to update these settings?").
							Value(&confirmed),
					),
				)

				err = form.Run()

				if err != nil {
					return err
				}

				// Convert strings back to ints
				input.BackupsRetentionDays, _ = strconv.Atoi(backupsRetentionDaysStr)
				input.IncrementalBackupsRetentionDays, _ = strconv.Atoi(incrementalBackupsRetentionDaysStr)
				input.QueryLogsRetentionDays, _ = strconv.Atoi(queryLogsRetentionDaysStr)
				input.ErrorLogsRetentionDays, _ = strconv.Atoi(errorLogsRetentionDaysStr)
			}

			if !confirmed {
				return nil
			}

			// Convert to JSON for API call
			data, err := json.Marshal(input)

			if err != nil {
				return err
			}

			var payload map[string]any
			if err := json.Unmarshal(data, &payload); err != nil {
				return err
			}

			res, apiErrors, err := api.Put(config, fmt.Sprintf("/v1/databases/%s/branches/%s/settings", databaseName, branchName), payload)

			if err != nil {
				return err
			}

			if apiErrors != nil {
				return apiErrors.Error()
			}

			_, err = lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.SuccessAlert(res["message"].(string)),
					components.DatabaseBranchSettingsCard(res["data"].(map[string]any)),
				),
			)

			return err
		},
	}

	cmd.Flags().Bool("backups-enabled", false, "Enable automated backups")
	cmd.Flags().String("backup-interval", "24h", "Backup interval (e.g., 24h, 48h)")
	cmd.Flags().Int("backups-retention-days", 30, "Days to retain backups")
	cmd.Flags().Bool("error-logs-enabled", true, "Enable error logging")
	cmd.Flags().Int("error-logs-retention-days", 15, "Days to retain error logs")
	cmd.Flags().Bool("incremental-backups-enabled", true, "Enable incremental backups")
	cmd.Flags().Int("incremental-backups-retention-days", 7, "Days to retain incremental backups")
	cmd.Flags().Bool("query-logs-enabled", true, "Enable query logging")
	cmd.Flags().Int("query-logs-retention-days", 15, "Days to retain query logs")
	cmd.Flags().String("foreign-keys", "", "Foreign keys pragma (ON or OFF)")

	return cmd
}
