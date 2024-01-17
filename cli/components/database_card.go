package components

func DatabaseCard(data map[string]interface{}) string {
	return NewCard(
		WithCardTitle("Database"),
		WithCardRows([]CardRow{
			{
				Key:   "ID",
				Value: data["id"].(string),
			},
			{
				Key:   "Name",
				Value: data["name"].(string),
			},
			{
				Key:   "URL",
				Value: data["url"].(string),
			},
		}),
	).View()
}
