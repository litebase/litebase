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
				"message": {
					Type: "string",
				},
				"data": {
					Type: "object",
				},
			},
			Required: []string{"status", "message"},
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
					Type: "object",
					// Map of fieldName: ["error message", ...]
					AdditionalProperties: &Schema{
						Type: "array",
						Items: &Schema{
							Type: "string",
						},
					},
				},
			},
		},
		"any": {
			Description: "A dynamic value that can be a string, number, boolean, null, array, or object. The actual type should be inferred from the associated ColumnDefinition type field.",
			OneOf: []*Schema{
				{Type: "string"},
				{Type: "number"},
				{Type: "integer"},
				{Type: "boolean"},
				{Type: "array", Items: &Schema{}},
				{Type: "object"},
				{Type: "null"},
			},
		},
	}
}
