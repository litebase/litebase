package components

import "fmt"

func DatabaseBranchSettingsCard(data map[string]any) string {
	rows := []CardRow{
		{
			Key:   "Backups Enabled",
			Value: fmt.Sprintf("%v", data["backupsEnabled"]),
		},
	}

	if data["backupsEnabled"].(bool) {
		rows = append(rows, CardRow{
			Key:   "Backup Interval",
			Value: data["backupInterval"].(string),
		})
		rows = append(rows, CardRow{
			Key:   "Backups Retention Days",
			Value: fmt.Sprintf("%.0f", data["backupsRetentionDays"].(float64)),
		})
	}

	rows = append(rows, CardRow{
		Key:   "Incremental Backups Enabled",
		Value: fmt.Sprintf("%v", data["incrementalBackupsEnabled"]),
	})

	if data["incrementalBackupsEnabled"].(bool) {
		rows = append(rows, CardRow{
			Key:   "Incremental Backups Retention Days",
			Value: fmt.Sprintf("%.0f", data["incrementalBackupsRetentionDays"].(float64)),
		})
	}

	rows = append(rows, CardRow{
		Key:   "Query Logs Enabled",
		Value: fmt.Sprintf("%v", data["queryLogsEnabled"]),
	})

	if data["queryLogsEnabled"].(bool) {
		rows = append(rows, CardRow{
			Key:   "Query Logs Retention Days",
			Value: fmt.Sprintf("%.0f", data["queryLogsRetentionDays"].(float64)),
		})
	}

	rows = append(rows, CardRow{
		Key:   "Error Logs Enabled",
		Value: fmt.Sprintf("%v", data["errorLogsEnabled"]),
	})

	if data["errorLogsEnabled"].(bool) {
		rows = append(rows, CardRow{
			Key:   "Error Logs Retention Days",
			Value: fmt.Sprintf("%.0f", data["errorLogsRetentionDays"].(float64)),
		})
	}

	if data["defaultPragmas"] != nil {
		pragmas := data["defaultPragmas"].(map[string]any)
		rows = append(rows, CardRow{
			Key:   "Foreign Keys",
			Value: pragmas["foreignKeys"].(string),
		})
	}

	return NewCard(
		WithCardTitle("Branch Settings"),
		WithCardRows(rows),
	).Render()
}
