package cmd

import (
	"fmt"
	"strconv"
	"time"

	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

func NewDatabaseErrorLogListCmd(config *config.CLIConfiguration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list <database/branch>",
		Args:  cobra.ExactArgs(1),
		Short: "List database error logs",
		Long: `List database error logs for a specific time range.

Error logs capture SQL execution failures including the statement,
error message, credential used, and execution latency.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			databaseName, branchName, err := splitDatabasePath(args[0])

			if err != nil {
				return fmt.Errorf("invalid database path: %w", err)
			}

			startStr, err := cmd.Flags().GetString("start")

			if err != nil {
				return fmt.Errorf("failed to get start time: %w", err)
			}

			endStr, err := cmd.Flags().GetString("end")

			if err != nil {
				return fmt.Errorf("failed to get end time: %w", err)
			}

			// Default to last hour if no time range specified
			if startStr == "" || endStr == "" {
				now := time.Now()

				if endStr == "" {
					endStr = strconv.FormatInt(now.Unix(), 10)
				}

				if startStr == "" {
					startStr = strconv.FormatInt(now.Add(-1*time.Hour).Unix(), 10)
				}
			}

			start, err := strconv.ParseUint(startStr, 10, 64)

			if err != nil {
				return fmt.Errorf("invalid start timestamp: %w", err)
			}

			end, err := strconv.ParseUint(endStr, 10, 64)

			if err != nil {
				return fmt.Errorf("invalid end timestamp: %w", err)
			}

			// Build query parameters
			queryParams := fmt.Sprintf("?start=%d&end=%d", start, end)

			data, err := api.Get(
				config,
				fmt.Sprintf("/v1/databases/%s/branches/%s/errors%s", databaseName, branchName, queryParams),
			)

			if err != nil {
				return err
			}

			if data["data"] == nil {
				_, err = lipgloss.Fprint(
					cmd.OutOrStdout(),
					components.Container(
						components.InfoAlert("No error entries found for the specified time range."),
					),
				)

				return err
			}

			errorEntries, ok := data["data"].([]any)

			if !ok {
				return fmt.Errorf("invalid response format: data is not an array")
			}

			// Build table headers
			headers := []string{
				"Timestamp", "Credential ID", "Error", "Statement", "Latency (ms)",
			}

			var rows [][]string

			for _, entryInterface := range errorEntries {
				entry, ok := entryInterface.(map[string]any)

				if !ok {
					continue
				}

				var row []string

				// Timestamp
				if timestamp, ok := entry["Timestamp"].(float64); ok {
					t := time.Unix(int64(timestamp), 0)
					row = append(row, t.Format("2006-01-02 15:04:05"))
				} else {
					row = append(row, fmt.Sprintf("%v", entry["Timestamp"]))
				}

				// Credential ID
				if credID, ok := entry["CredentialID"].(string); ok {
					if len(credID) > 12 {
						row = append(row, credID[:12]+"...")
					} else {
						row = append(row, credID)
					}
				} else {
					row = append(row, fmt.Sprintf("%v", entry["CredentialID"]))
				}

				// Error message
				if errMsg, ok := entry["Error"].(string); ok {
					// Truncate long error messages
					if len(errMsg) > 50 {
						row = append(row, errMsg[:50]+"...")
					} else {
						row = append(row, errMsg)
					}
				} else {
					row = append(row, fmt.Sprintf("%v", entry["Error"]))
				}

				// Statement
				if stmt, ok := entry["Statement"].(string); ok {
					// Truncate long statements
					if len(stmt) > 40 {
						row = append(row, stmt[:40]+"...")
					} else {
						row = append(row, stmt)
					}
				} else {
					row = append(row, fmt.Sprintf("%v", entry["Statement"]))
				}

				// Latency
				if latency, ok := entry["Latency"].(float64); ok {
					row = append(row, fmt.Sprintf("%.2f", latency))
				} else {
					row = append(row, fmt.Sprintf("%v", entry["Latency"]))
				}

				rows = append(rows, row)
			}

			_, err = lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.NewTable(headers, rows).
						Render(config.GetInteractive()),
				),
			)

			return err
		},
	}

	// Add flags for time range
	cmd.Flags().String("start", "", "Start timestamp (Unix seconds). Defaults to 1 hour ago")
	cmd.Flags().String("end", "", "End timestamp (Unix seconds). Defaults to now")

	return cmd
}

func NewDatabaseErrorLogCmd(config *config.CLIConfiguration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "error-logs",
		Short: "View database error logs",
		RunE: func(cmd *cobra.Command, args []string) error {
			err := cmd.Help()

			if err != nil {
				return err
			}

			return nil
		},
	}

	cmd.AddCommand(NewDatabaseErrorLogListCmd(config))

	return cmd
}
