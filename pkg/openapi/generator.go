package openapi

import (
	"encoding/json"
	"fmt"
	"maps"
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// Generator generates OpenAPI specifications from router definitions
type Generator struct {
	analyzer *RouteAnalyzer
	spec     *OpenAPISpec
}

// NewGenerator creates a new OpenAPI generator
func NewGenerator() *Generator {
	return &Generator{
		analyzer: NewRouteAnalyzer(),
		spec: &OpenAPISpec{
			OpenAPI: "3.1.0",
			Info: Info{
				Title:       "Litebase Database API",
				Description: "Litebase is a distributed SQLite database management system with branching capabilities. This API provides comprehensive access to database operations, user management, access control, and cluster administration.",
				Version:     "1.0.0",
				Contact: &Contact{
					Name: "Litebase Support",
					URL:  "https://github.com/litebase/litebase",
				},
				License: &License{
					Name: "MIT",
					URL:  "https://opensource.org/licenses/MIT",
				},
			},
			Servers: []Server{
				{
					URL:         "http://localhost:8080",
					Description: "Local development server",
				},
			},
			Paths: make(map[string]PathItem),
			Components: &Components{
				SecuritySchemes: map[string]SecurityScheme{
					"AccessKeyAuth": {
						Type:         "http",
						Scheme:       "Litebase-HMAC-SHA256",
						Description:  "HMAC-SHA256 authentication using access id and secret",
						BearerFormat: "Litebase-HMAC-SHA256",
					},
					"BasicAuth": {
						Type:        "http",
						Scheme:      "basic",
						Description: "Basic authentication for root user",
					},
					"TokenAuth": {
						Type:         "http",
						Scheme:       "bearer",
						Description:  "Bearer token authentication using temporary token",
						BearerFormat: "JWT",
					},
				},
				Schemas: map[string]*Schema{
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
				},
			},
			Security: []SecurityRequirement{
				{
					"AccessKeyAuth": []string{},
				},
				{
					"BasicAuth": []string{},
				},
				{
					"TokenAuth": []string{},
				},
			},
			Tags: []Tag{
				{Name: "Health", Description: "Health check endpoints"},
				{Name: "Cluster", Description: "Cluster management and status operations"},
				{Name: "Users", Description: "User management operations"},
				{Name: "Access Keys", Description: "Access key management for authentication"},
				{Name: "Databases", Description: "Database management operations"},
				{Name: "Database Branches", Description: "Database branch management operations"},
				{Name: "Queries", Description: "SQL query execution operations"},
				{Name: "Backups", Description: "Database backup and restore operations"},
				{Name: "Snapshots", Description: "Database snapshot operations"},
				{Name: "Metrics", Description: "Performance and usage metrics"},
				{Name: "Keys", Description: "Encryption key management"},
				{Name: "Events", Description: "Internal cluster event management"},
			},
		},
	}
}

// GenerateFromRoutes generates OpenAPI spec from a routes map
func (g *Generator) GenerateFromRoutes(routes map[string]map[string]any) error {
	for method, pathMap := range routes {
		for path, route := range pathMap {
			if err := g.processRoute(method, path, route); err != nil {
				return err
			}
		}
	}

	return nil
}

// processRoute processes a single route and adds it to the spec
func (g *Generator) processRoute(method, path string, route any) error {
	// Convert path to OpenAPI format
	openAPIPath := ConvertPathToOpenAPI(path)

	// Get or create path item
	pathItem, exists := g.spec.Paths[openAPIPath]

	if !exists {
		pathItem = PathItem{}
	}

	// Extract path parameters
	pathParams := ExtractPathParameters(path)

	if len(pathParams) > 0 {
		pathItem.Parameters = pathParams
	}

	// Create operation
	operation := Operation{
		Responses: make(map[string]Response),
	}

	// If route has OpenAPI metadata, use it
	if routeStruct, ok := route.(interface{ GetOpenAPIMetadata() *OpenAPIMetadata }); ok {
		metadata := routeStruct.GetOpenAPIMetadata()

		if metadata != nil {
			operation.Summary = metadata.Summary
			operation.Description = metadata.Description
			operation.OperationID = metadata.OperationID
			operation.Tags = metadata.Tags
			operation.Parameters = metadata.Parameters
			operation.RequestBody = metadata.RequestBody
			operation.Security = metadata.Security

			// Copy responses from metadata
			maps.Copy(operation.Responses, metadata.Responses)
		}
	}

	// If no responses defined, add default ones
	if len(operation.Responses) == 0 {
		operation.Responses = g.getDefaultResponses(method)
	}

	// Set default operation info if not provided
	if operation.Summary == "" {
		operation.Summary = g.generateDefaultSummary(method, path)
	}

	if operation.OperationID == "" {
		operation.OperationID = g.generateOperationID(method, path)
	}

	if len(operation.Tags) == 0 {
		operation.Tags = g.generateDefaultTags(path)
	}

	// Add security if not specified
	if len(operation.Security) == 0 && !strings.Contains(path, "/health") && !strings.Contains(path, "/cluster") {
		operation.Security = []SecurityRequirement{
			{"AccessKeyAuth": []string{}},
			{"BasicAuth": []string{}},
			{"TokenAuth": []string{}},
		}
	}

	// Set the operation on the path item
	switch strings.ToLower(method) {
	case "get":
		pathItem.Get = &operation
	case "post":
		pathItem.Post = &operation
	case "put":
		pathItem.Put = &operation
	case "delete":
		pathItem.Delete = &operation
	case "patch":
		pathItem.Patch = &operation
	case "options":
		pathItem.Options = &operation
	case "head":
		pathItem.Head = &operation
	}

	g.spec.Paths[openAPIPath] = pathItem

	return nil
}

// getDefaultResponses returns default responses for a method
func (g *Generator) getDefaultResponses(method string) map[string]Response {
	responses := make(map[string]Response)

	// Common error responses
	responses["400"] = Response{
		Description: "Bad request - invalid input or parameters",
		Content: map[string]MediaType{
			"application/json": {
				Schema: &Schema{Ref: "#/components/schemas/ErrorResponse"},
			},
		},
	}

	responses["401"] = Response{
		Description: "Unauthorized - authentication required",
		Content: map[string]MediaType{
			"application/json": {
				Schema: &Schema{Ref: "#/components/schemas/ErrorResponse"},
			},
		},
	}

	responses["500"] = Response{
		Description: "Internal server error",
		Content: map[string]MediaType{
			"application/json": {
				Schema: &Schema{Ref: "#/components/schemas/ErrorResponse"},
			},
		},
	}

	// Method-specific success responses
	switch strings.ToUpper(method) {
	case "GET":
		responses["200"] = Response{
			Description: "Successful operation",
			Content: map[string]MediaType{
				"application/json": {
					Schema: &Schema{Ref: "#/components/schemas/SuccessResponse"},
				},
			},
		}
		responses["404"] = Response{
			Description: "Resource not found",
			Content: map[string]MediaType{
				"application/json": {
					Schema: &Schema{Ref: "#/components/schemas/ErrorResponse"},
				},
			},
		}
	case "POST":
		responses["201"] = Response{
			Description: "Resource created successfully",
			Content: map[string]MediaType{
				"application/json": {
					Schema: &Schema{Ref: "#/components/schemas/SuccessResponse"},
				},
			},
		}
		responses["422"] = Response{
			Description: "Validation error",
			Content: map[string]MediaType{
				"application/json": {
					Schema: &Schema{Ref: "#/components/schemas/ValidationErrorResponse"},
				},
			},
		}
	case "PUT":
		responses["200"] = Response{
			Description: "Resource updated successfully",
			Content: map[string]MediaType{
				"application/json": {
					Schema: &Schema{Ref: "#/components/schemas/SuccessResponse"},
				},
			},
		}
		responses["404"] = Response{
			Description: "Resource not found",
			Content: map[string]MediaType{
				"application/json": {
					Schema: &Schema{Ref: "#/components/schemas/ErrorResponse"},
				},
			},
		}
	case "DELETE":
		responses["204"] = Response{
			Description: "Resource deleted successfully",
		}
		responses["404"] = Response{
			Description: "Resource not found",
			Content: map[string]MediaType{
				"application/json": {
					Schema: &Schema{Ref: "#/components/schemas/ErrorResponse"},
				},
			},
		}
	}

	return responses
}

// generateDefaultSummary generates a default summary for an operation
func (g *Generator) generateDefaultSummary(method, path string) string {
	// Extract resource from path
	parts := strings.Split(strings.Trim(path, "/"), "/")
	resource := ""

	// Find the main resource (usually after v1)
	for i, part := range parts {
		if part == "v1" && i+1 < len(parts) {
			resource = parts[i+1]
			break
		}
	}

	if resource == "" && len(parts) > 0 {
		resource = parts[len(parts)-1]
	}

	// Remove hyphens and convert to title case
	resource = strings.ReplaceAll(resource, "-", " ")

	if resource != "" {
		resource = strings.ToUpper(resource[:1]) + resource[1:]
	}

	switch strings.ToUpper(method) {
	case "GET":
		if strings.Contains(path, "{") {
			return fmt.Sprintf("Get %s details", strings.TrimSuffix(resource, "s"))
		}

		return fmt.Sprintf("List %s", resource)
	case "POST":
		return fmt.Sprintf("Create %s", strings.TrimSuffix(resource, "s"))
	case "PUT":
		return fmt.Sprintf("Update %s", strings.TrimSuffix(resource, "s"))
	case "DELETE":
		return fmt.Sprintf("Delete %s", strings.TrimSuffix(resource, "s"))
	default:
		return fmt.Sprintf("%s %s", strings.Title(strings.ToLower(method)), resource)
	}
}

// generateOperationID generates an operation ID for an operation
func (g *Generator) generateOperationID(method, path string) string {
	// Extract resource from path
	parts := strings.Split(strings.Trim(path, "/"), "/")
	resource := ""
	action := ""

	// Find the main resource (usually after v1)
	for i, part := range parts {
		if part == "v1" && i+1 < len(parts) {
			resource = parts[i+1]
			if i+2 < len(parts) && !strings.Contains(parts[i+2], "{") {
				action = parts[i+2]
			}
			break
		}
	}

	if resource == "" && len(parts) > 0 {
		resource = parts[len(parts)-1]
	}

	// Convert to camelCase
	resource = toCamelCase(resource)

	if action != "" {
		action = toCamelCase(action)
	}

	methodPrefix := ""

	switch strings.ToUpper(method) {
	case "GET":
		if strings.Contains(path, "{") {
			methodPrefix = "get"
		} else {
			methodPrefix = "list"
		}
	case "POST":
		methodPrefix = "create"
	case "PUT":
		methodPrefix = "update"
	case "DELETE":
		methodPrefix = "delete"
	default:
		methodPrefix = strings.ToLower(method)
	}

	if action != "" {
		return fmt.Sprintf("%s%s%s", methodPrefix, resource, strings.Title(action))
	}

	return fmt.Sprintf("%s%s", methodPrefix, resource)
}

// generateDefaultTags generates default tags for an operation
func (g *Generator) generateDefaultTags(path string) []string {
	parts := strings.Split(strings.Trim(path, "/"), "/")

	// Find the main resource (usually after v1)
	for i, part := range parts {
		if part == "v1" && i+1 < len(parts) {
			resource := parts[i+1]

			// Convert resource name to tag format
			switch resource {
			case "users":
				return []string{"Users"}
			case "access-keys":
				return []string{"Access Keys"}
			case "databases":
				if i+2 < len(parts) && parts[i+2] != "{databaseName}" {
					subResource := parts[i+2]
					switch subResource {
					case "branches":
						return []string{"Database Branches"}
					case "backups":
						return []string{"Backups"}
					case "snapshots":
						return []string{"Snapshots"}
					}
				}

				return []string{"Databases"}
			case "cluster":
				return []string{"Cluster"}
			case "keys":
				return []string{"Keys"}
			case "events":
				return []string{"Events"}
			case "health":
				return []string{"Health"}
			case "tokens":
				return []string{"Tokens"}
			default:
				return []string{cases.Title(language.Und).String(strings.ReplaceAll(resource, "-", " "))}
			}
		}
	}

	return []string{"Other"}
}

// toCamelCase converts a string to camelCase
func toCamelCase(s string) string {
	if s == "" {
		return s
	}

	// Split on hyphens and underscores
	parts := strings.FieldsFunc(s, func(c rune) bool {
		return c == '-' || c == '_'
	})

	if len(parts) == 0 {
		return s
	}

	// First part stays lowercase, rest are title case
	result := strings.ToLower(parts[0])
	for i := 1; i < len(parts); i++ {
		if len(parts[i]) > 0 {
			result += strings.ToUpper(parts[i][:1]) + strings.ToLower(parts[i][1:])
		}
	}

	return result
}

// ToJSON converts the OpenAPI spec to JSON
func (g *Generator) ToJSON(indent bool) ([]byte, error) {
	if indent {
		return json.MarshalIndent(g.spec, "", "  ")
	}
	return json.Marshal(g.spec)
}
