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

func NewDatabaseQueryLogListCmd(config *config.CLIConfiguration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list <database/branch>",
		Args:  cobra.ExactArgs(1),
		Short: "List database query metrics",
		Long: `List database query metrics for a specific time range.

Query metrics include performance statistics like latency percentiles, 
execution count, and timing information for database queries.`,
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

			step, err := cmd.Flags().GetInt64("step")

			if err != nil {
				return fmt.Errorf("failed to get step: %w", err)
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

			if step < 1 {
				return fmt.Errorf("step must be at least 1 second")
			}

			// Build query parameters
			queryParams := fmt.Sprintf("?start=%d&end=%d&step=%d", start, end, step)

			data, err := api.Get(config, fmt.Sprintf("/v1/databases/%s/%s/metrics/query%s",
				databaseName, branchName, queryParams))
			if err != nil {
				return err
			}

			if data["data"] == nil {
				_, err = lipgloss.Fprint(
					cmd.OutOrStdout(),
					components.Container(
						components.InfoAlert("No query metrics found for the specified time range."),
					),
				)

				return err
			}

			// Get metadata
			meta, ok := data["meta"].(map[string]any)

			if !ok {
				return fmt.Errorf("invalid response format: missing meta")
			}

			keysInterface, ok := meta["keys"].([]any)

			if !ok {
				return fmt.Errorf("invalid response format: missing keys in meta")
			}

			var keys []string

			for _, key := range keysInterface {
				if keyStr, ok := key.(string); ok {
					keys = append(keys, keyStr)
				}
			}

			metricsData, ok := data["data"].([]any)

			if !ok {
				return fmt.Errorf("invalid response format: data is not an array")
			}

			// Build table headers - display user-friendly names
			headers := []string{
				"Query ID", "Count", "Avg Latency (ms)", "Min Latency (ms)",
				"Max Latency (ms)", "P50 (ms)", "P90 (ms)", "P99 (ms)", "Timestamp",
			}

			var rows [][]string

			for _, metricInterface := range metricsData {
				metric, ok := metricInterface.([]any)

				if !ok {
					continue
				}

				if len(metric) != len(keys) {
					continue
				}

				var row []string

				for i, value := range metric {
					switch i {
					case 0: // Query ID (checksum)
						if str, ok := value.(string); ok {
							// Truncate long hex checksums for display
							if len(str) > 12 {
								row = append(row, str[:12]+"...")
							} else {
								row = append(row, str)
							}
						} else {
							row = append(row, fmt.Sprintf("%v", value))
						}
					case 8: // Timestamp
						if timestamp, ok := value.(float64); ok {
							t := time.Unix(int64(timestamp), 0)
							row = append(row, t.Format("15:04:05"))
						} else {
							row = append(row, fmt.Sprintf("%v", value))
						}
					case 2, 3, 4, 5, 6, 7: // Latency fields - convert to milliseconds
						if latency, ok := value.(float64); ok {
							row = append(row, fmt.Sprintf("%.2f", latency*1000))
						} else {
							row = append(row, fmt.Sprintf("%v", value))
						}
					default:
						row = append(row, fmt.Sprintf("%v", value))
					}
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

	// Add flags for time range and aggregation
	cmd.Flags().String("start", "", "Start timestamp (Unix seconds). Defaults to 1 hour ago")
	cmd.Flags().String("end", "", "End timestamp (Unix seconds). Defaults to now")
	cmd.Flags().Int64("step", 1, "Aggregation step in seconds (default: 1)")

	return cmd
}

func NewDatabaseQueryLogCmd(config *config.CLIConfiguration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query-logs",
		Short: "View database query logs and metrics",
		RunE: func(cmd *cobra.Command, args []string) error {
			err := cmd.Help()

			if err != nil {
				return err
			}

			return nil
		},
	}

	cmd.AddCommand(NewDatabaseQueryLogListCmd(config))

	return cmd
}
