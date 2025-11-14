package components

import (
	"time"
)

func DatabaseCard(data map[string]any) string {
	rows := []CardRow{}

	if databaseId, ok := data["databaseId"].(string); ok {
		rows = append(rows, CardRow{
			Key:   "ID",
			Value: databaseId,
		})
	}

	if databaseName, ok := data["databaseName"].(string); ok {
		rows = append(rows, CardRow{
			Key:   "Database Name",
			Value: databaseName,
		})
	}

	if branchName, ok := data["branchName"].(string); ok {
		rows = append(rows, CardRow{
			Key:   "Branch Name",
			Value: branchName,
		})
	}

	if data["createdAt"] != nil {
		if createdAtStr, ok := data["createdAt"].(string); ok {
			parsedDate, err := time.Parse(time.RFC3339, createdAtStr)

			if err == nil {
				rows = append(rows, CardRow{
					Key:   "Created At",
					Value: parsedDate.Format(time.RFC3339),
				})
			}
		}
	}

	if data["updatedAt"] != nil {
		if updatedAtStr, ok := data["updatedAt"].(string); ok {
			parsedDate, err := time.Parse(time.RFC3339, updatedAtStr)

			if err == nil {
				rows = append(rows, CardRow{
					Key:   "Updated At",
					Value: parsedDate.Format(time.RFC3339),
				})
			}
		}
	}

	if url, ok := data["url"].(string); ok && url != "" {
		rows = append(rows, CardRow{
			Key:   "URL",
			Value: url,
		})
	}

	return NewCard(
		WithCardTitle("Database"),
		WithCardRows(rows),
	).Render()
}
