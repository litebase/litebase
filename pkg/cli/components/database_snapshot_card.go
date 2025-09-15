package components

import (
	"fmt"
	"time"
)

func DatabaseSnapshotCard(data map[string]any) string {
	// Handle timestamp as int64 to avoid scientific notation
	var timestampStr string

	if ts, ok := data["timestamp"].(float64); ok {
		timestampStr = fmt.Sprintf("%.0f", ts)
	} else {
		timestampStr = fmt.Sprintf("%v", data["timestamp"])
	}

	rows := []CardRow{
		{
			Key:   "Timestamp",
			Value: timestampStr,
		},
		{
			Key:   "Database ID",
			Value: data["database_id"].(string),
		},
		{
			Key:   "Branch ID",
			Value: data["database_branch_id"].(string),
		},
	}

	// Add restore points summary information
	var restorePointsTable string
	if restorePoints, exists := data["restore_points"]; exists && restorePoints != nil {
		if rpMap, ok := restorePoints.(map[string]any); ok {
			if total, totalExists := rpMap["total"]; totalExists {
				rows = append(rows, CardRow{
					Key:   "Total Restore Points",
					Value: fmt.Sprintf("%v", total),
				})
			}
			if start, startExists := rpMap["start"]; startExists {
				if startInt, ok := start.(float64); ok {
					startTime := time.Unix(0, int64(startInt)).UTC()
					rows = append(rows, CardRow{
						Key:   "Earliest Restore Point",
						Value: startTime.Format(time.RFC3339),
					})
				}
			}

			if end, endExists := rpMap["end"]; endExists {
				if endInt, ok := end.(float64); ok {
					endTime := time.Unix(0, int64(endInt)).UTC()
					rows = append(rows, CardRow{
						Key:   "Latest Restore Point",
						Value: endTime.Format(time.RFC3339),
					})
				}
			}

			// Create table of all restore points if data array exists
			if rpData, dataExists := rpMap["data"]; dataExists {
				if dataArray, ok := rpData.([]any); ok && len(dataArray) > 0 {
					restorePointRows := [][]string{}
					
					for i, rp := range dataArray {
						if rpTimestamp, ok := rp.(float64); ok {
							timestamp := fmt.Sprintf("%.0f", rpTimestamp)
							rpTime := time.Unix(0, int64(rpTimestamp)).UTC()
							timeFormatted := rpTime.Format(time.RFC3339)
							
							restorePointRows = append(restorePointRows, []string{
								fmt.Sprintf("%d", i+1),
								timestamp,
								timeFormatted,
							})
						}
					}
					
					if len(restorePointRows) > 0 {
						restorePointsTable = NewTable(
							[]string{"#", "Timestamp", "Time (UTC)"},
							restorePointRows,
						).Render(true) // Always render interactive for better formatting
					}
				}
			}
		}
	}

	card := NewCard(
		WithCardTitle("Database Snapshot"),
		WithCardRows(rows),
	).Render()

	// If we have restore points table, append it with a proper title
	if restorePointsTable != "" {
		restorePointsTitle := CardTitleStyle().Render("Restore Points")
		card += "\n\n" + restorePointsTitle + "\n\n" + restorePointsTable
	}

	return card
}
