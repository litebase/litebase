package components

import "time"

func DatabaseBranchCard(data map[string]any) string {
	rows := []CardRow{
		{
			Key:   "ID",
			Value: data["databaseBranchId"].(string),
		},
		{
			Key:   "Name",
			Value: data["name"].(string),
		},
		{
			Key:   "Database ID",
			Value: data["databaseId"].(string),
		},
	}

	if data["parentName"] != nil {
		rows = append(rows, CardRow{
			Key:   "Parent Branch",
			Value: data["parentName"].(string),
		})
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

	return NewCard(
		WithCardTitle("Database Branch"),
		WithCardRows(rows),
	).Render()
}
