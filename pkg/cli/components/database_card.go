package components

import "time"

func DatabaseCard(data map[string]any) string {
	rows := []CardRow{
		{
			Key:   "ID",
			Value: data["id"].(string),
		},
		{
			Key:   "Name",
			Value: data["name"].(string),
		},
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

	rows = append(rows, CardRow{
		Key:   "",
		Value: "",
	}, CardRow{
		Key:   "URL",
		Value: data["url"].(string),
	})

	return NewCard(
		WithCardTitle("Database"),
		WithCardRows(rows),
	).Render()
}
