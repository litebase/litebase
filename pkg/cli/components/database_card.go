package components

import (
	"time"
)

func DatabaseCard(data map[string]any) string {
	rows := []CardRow{
		{
			Key:   "ID",
			Value: data["databaseId"].(string),
		},
		{
			Key:   "Database Name",
			Value: data["databaseName"].(string),
		},

		{
			Key:   "Branch Name",
			Value: data["branchName"].(string),
		},
	}

	if data["createdAt"] != nil {
		parsedDate, err := time.Parse(time.RFC3339, data["createdAt"].(string))

		if err == nil {
			rows = append(rows, CardRow{
				Key:   "Created At",
				Value: parsedDate.Format(time.RFC3339),
			})
		}
	}

	if data["updatedAt"] != nil {
		parsedDate, err := time.Parse(time.RFC3339, data["updatedAt"].(string))

		if err == nil {
			rows = append(rows, CardRow{
				Key:   "Updated At",
				Value: parsedDate.Format(time.RFC3339),
			})
		}
	}

	if url, ok := data["url"].(string); ok && url != "" {
		rows = append(rows, CardRow{
			Key:   "URL",
			Value: data["url"].(string),
		})
	}

	return NewCard(
		WithCardTitle("Database"),
		WithCardRows(rows),
	).Render()
}
