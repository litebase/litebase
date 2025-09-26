package openapi

// GetSecuritySchemes returns the security scheme definitions for the OpenAPI specification
func GetSecuritySchemes() map[string]SecurityScheme {
	return map[string]SecurityScheme{
		"AccessKeyAuth": {
			Type:        "http",
			Scheme:      "litebase-hmac-SHA256",
			Description: "HMAC-SHA256 authentication using access id and secret",
		},
		"BasicAuth": {
			Type:        "http",
			Scheme:      "basic",
			Description: "Basic authentication for root user",
		},
		"TokenAuth": {
			Type:        "http",
			Scheme:      "bearer",
			Description: "Bearer token authentication using temporary token",
		},
	}
}

// GetGlobalSecurityRequirements returns the global security requirements for authenticated endpoints
func GetGlobalSecurityRequirements() []SecurityRequirement {
	return []SecurityRequirement{
		{
			"AccessKeyAuth": []string{},
		},
		{
			"BasicAuth": []string{},
		},
		{
			"TokenAuth": []string{},
		},
	}
}
