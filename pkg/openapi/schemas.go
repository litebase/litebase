package openapi

// GetCommonSchemas returns the common schema definitions for the OpenAPI specification
func GetCommonSchemas() map[string]*Schema {
	return map[string]*Schema{
		"SuccessResponse": {
			Type: "object",
			Properties: map[string]*Schema{
				"status": {
					Type:    "string",
					Example: "success",
				},
				"data": {
					Type: "object",
				},
			},
			Required: []string{"status"},
		},
		"ErrorResponse": {
			Type: "object",
			Properties: map[string]*Schema{
				"status": {
					Type:    "string",
					Example: "error",
				},
				"message": {
					Type: "string",
				},
				"code": {
					Type: "string",
				},
			},
			Required: []string{"status", "message"},
		},
		"ValidationErrorResponse": {
			Type: "object",
			Properties: map[string]*Schema{
				"status": {
					Type:    "string",
					Example: "error",
				},
				"message": {
					Type: "string",
				},
				"errors": {
					Type: "array",
					Items: &Schema{
						Type: "object",
						Properties: map[string]*Schema{
							"field": {
								Type: "string",
							},
							"message": {
								Type: "string",
							},
						},
					},
				},
			},
		},
	}
}