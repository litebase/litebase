package cmd

import (
	"fmt"
	"time"

	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

func NewDatabaseSnapshotListCmd(config *config.CLIConfiguration) *cobra.Command {
	return &cobra.Command{
		Use:   "list <database/branch>",
		Args:  cobra.ExactArgs(1),
		Short: "List database snapshots",
		RunE: func(cmd *cobra.Command, args []string) error {
			databaseName, branchName, err := splitDatabasePath(args[0])

			if err != nil {
				return fmt.Errorf("invalid database path: %w", err)
			}

			data, err := api.Get(config, fmt.Sprintf("/v1/databases/%s/branches/%s/snapshots", databaseName, branchName))

			if err != nil {
				return err
			}

			if data["data"] == nil {
				rows := [][]string{}

				_, err = lipgloss.Fprint(
					cmd.OutOrStdout(),
					components.Container(
						components.NewTable([]string{"Date (UTC)", "Restore Points", "Start Restore Point", "Last Restore Point"}, rows).
							Render(config.GetInteractive()),
					),
				)

				return err
			}

			rows := [][]string{}

			for _, snapshot := range data["data"].([]any) {
				snapshotData := snapshot.(map[string]any)

				// Convert timestamp to UTC date for display
				var dateUTC string
				if ts, ok := snapshotData["timestamp"].(float64); ok {
					t := time.Unix(0, int64(ts)).UTC()
					dateUTC = t.Format("2006-01-02 15:04:05")
				} else {
					dateUTC = "Unknown"
				}

				restorePointsCount := "0"
				startRestorePoint := "-"
				lastRestorePoint := "-"

				if restorePoints, exists := snapshotData["restorePoints"]; exists && restorePoints != nil {
					if rpMap, ok := restorePoints.(map[string]any); ok {
						if total, totalExists := rpMap["total"]; totalExists {
							restorePointsCount = fmt.Sprintf("%v", total)
						}
						if start, startExists := rpMap["start"]; startExists {
							if startInt, ok := start.(float64); ok {
								startTime := time.Unix(0, int64(startInt)).UTC()
								startRestorePoint = startTime.Format("2006-01-02 15:04:05")
							}
						}
						if end, endExists := rpMap["end"]; endExists {
							if endInt, ok := end.(float64); ok {
								endTime := time.Unix(0, int64(endInt)).UTC()
								lastRestorePoint = endTime.Format("2006-01-02 15:04:05")
							}
						}
					}
				}

				rows = append(rows, []string{
					dateUTC,
					restorePointsCount,
					startRestorePoint,
					lastRestorePoint,
				})
			}

			_, err = lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.NewTable([]string{"Date (UTC)", "Restore Points", "Start Restore Point", "Last Restore Point"}, rows).
						Render(config.GetInteractive()),
				),
			)

			return err
		},
	}
}
