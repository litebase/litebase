package components

import "time"

func DatabaseBranchCard(data map[string]any) string {
	rows := []CardRow{
		{
			Key:   "ID",
			Value: data["database_branch_id"].(string),
		},
		{
			Key:   "Name",
			Value: data["name"].(string),
		},
		{
			Key:   "Database ID",
			Value: data["database_id"].(string),
		},
	}

	if data["parent_name"] != nil {
		rows = append(rows, CardRow{
			Key:   "Parent Branch",
			Value: data["parent_name"].(string),
		})
	}

	if data["created_at"] != nil {
		parsedDate, err := time.Parse(time.RFC3339, data["created_at"].(string))

		if err == nil {
			rows = append(rows, CardRow{
				Key:   "Created At",
				Value: parsedDate.Format(time.RFC3339),
			})
		}
	}

	if data["updated_at"] != nil {
		parsedDate, err := time.Parse(time.RFC3339, data["updated_at"].(string))

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
