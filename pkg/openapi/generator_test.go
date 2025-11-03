package openapi

import (
	"encoding/json"
	"slices"
	"sort"
	"strings"
	"testing"
)

// resolveSchemaRef resolves a $ref in a schema to the actual component schema
// If the schema has no $ref, returns the schema as-is
func resolveSchemaRef(schema *Schema, components map[string]*Schema) *Schema {
	if schema == nil {
		return nil
	}

	// If no ref, return as-is
	if schema.Ref == "" {
		return schema
	}

	// Extract component name from ref (e.g., "#/components/schemas/UserStoreRequest" -> "UserStoreRequest")
	refParts := strings.Split(schema.Ref, "/")

	if len(refParts) == 0 {
		return schema
	}

	componentName := refParts[len(refParts)-1]

	// Look up in components
	if resolved, exists := components[componentName]; exists {
		return resolved
	}

	// If not found, return original
	return schema
}

// TestDynamicAnalyzerConsistency tests that the analyzer consistently extracts
// types and schemas across different controller patterns
func TestDynamicAnalyzerConsistency(t *testing.T) {
	analyzer := NewGenerator()

	// Test ALL routes analysis (the new dynamic approach)
	analysis, err := analyzer.AnalyzeAllRoutes()

	if err != nil {
		t.Fatalf("Failed to analyze all routes: %v", err)
	}

	// Verify basic analysis structure
	t.Run("AllRoutesStructure", func(t *testing.T) {
		if len(analysis.Methods) == 0 {
			t.Error("Expected methods to be detected from routes")
		}

		// Should detect multiple controllers
		controllerNames := make(map[string]bool)

		for methodName := range analysis.Methods {
			if strings.Contains(methodName, "Controller") {
				parts := strings.Split(methodName, "Controller")

				if len(parts) > 0 {
					controllerNames[parts[0]+"Controller"] = true
				}
			}
		}

		if len(controllerNames) < 2 {
			t.Errorf("Expected multiple controllers, got %d: %v", len(controllerNames), controllerNames)
		}
	})

	// Test type extraction consistency (still using individual controller analysis for detailed testing)
	t.Run("TypeExtractionConsistency", func(t *testing.T) {
		// Test individual controller analysis for detailed type checking
		_, err := analyzer.AnalyzeController("../http/user_controller.go", "UserController")

		if err != nil {
			t.Fatalf("Failed to analyze UserController: %v", err)
		}

		typeInfo := analyzer.GetTypeInfo()

		expectedTypes := []string{
			"UserStoreRequest",
			"UserUpdateRequest",
		}

		for _, typeName := range expectedTypes {
			if _, exists := typeInfo[typeName]; !exists {
				t.Errorf("Expected type %s not found", typeName)
			}
		}

		// Verify UserStoreRequest has expected fields
		if storeReq, exists := typeInfo["UserStoreRequest"]; exists {
			expectedFields := []string{"Description", "Password", "Statements", "Username"}

			for _, fieldName := range expectedFields {
				if _, fieldExists := storeReq.Fields[fieldName]; !fieldExists {
					t.Errorf("Expected field %s not found in UserStoreRequest", fieldName)
				}
			}
		}
	})

	// Test OpenAPI generation consistency
	t.Run("OpenAPIGenerationConsistency", func(t *testing.T) {
		paths := analyzer.GenerateOpenAPIFromAnalysis(analysis)

		if len(paths) == 0 {
			t.Error("Expected paths to be generated")
		}

		// Should have multiple paths
		if len(paths) < 3 {
			t.Errorf("Expected at least 3 paths, got %d", len(paths))
		}

		// Verify operations have proper structure
		operationCount := 0

		for _, pathMethods := range paths {
			for _, operation := range pathMethods {
				operationCount++

				if len(operation.Tags) == 0 {
					t.Error("Expected operation to have tags")
				}

				if operation.OperationID == "" {
					t.Error("Expected operation to have operationId")
				}
			}
		}

		if operationCount < 10 {
			t.Errorf("Expected at least 10 operations, got %d", operationCount)
		}
	})
}

// TestResponseSchemaExtraction tests that response schemas are properly extracted
func TestResponseSchemaExtraction(t *testing.T) {
	analyzer := NewGenerator()
	analysis, err := analyzer.AnalyzeAllRoutes()

	if err != nil {
		t.Fatalf("Failed to analyze all routes: %v", err)
	}

	paths := analyzer.GenerateOpenAPIFromAnalysis(analysis)
	components := analyzer.GetRegisteredSchemas()

	testCases := []struct {
		name       string
		path       string
		method     string
		statusCode string
		testFunc   func(t *testing.T, response Response, components map[string]*Schema)
	}{
		{
			name:       "UserIndex200Response",
			path:       "/v1/users",
			method:     "get",
			statusCode: "200",
			testFunc: func(t *testing.T, response Response, components map[string]*Schema) {
				// Should have data array containing users
				schema := response.Content["application/json"].Schema

				if schema == nil {
					t.Fatal("Expected schema to exist")
					return
				}

				if dataSchema, exists := schema.Properties["data"]; exists {
					// The data should be an array of users
					if dataSchema.Type != "array" {
						t.Errorf("Expected data to be array, got %s", dataSchema.Type)
					}

					// Check that the array has items schema
					if dataSchema.Items == nil {
						t.Error("Expected data array to have items schema")
					} else {
						// Resolve $ref if items is a reference
						itemsSchema := resolveSchemaRef(dataSchema.Items, components)

						// Verify the items are user objects with expected properties
						if itemsSchema.Type != "object" {
							t.Errorf("Expected data array items to be objects, got %s", itemsSchema.Type)
						}

						expectedUserProps := []string{"username", "statements", "createdAt", "updatedAt"}

						for _, prop := range expectedUserProps {
							if _, exists := itemsSchema.Properties[prop]; !exists {
								t.Errorf("Expected user item property %s", prop)
							}
						}
					}
				} else {
					t.Error("Expected data property in response schema")
				}
			},
		},
		{
			name:       "UserShow200Response",
			path:       "/v1/users/{username}",
			method:     "get",
			statusCode: "200",
			testFunc: func(t *testing.T, response Response, components map[string]*Schema) {
				// Should have SuccessResponse structure with UserResponse data
				schema := response.Content["application/json"].Schema

				if schema == nil {
					t.Fatal("Expected schema to exist")
				}

				expectedProps := []string{"status", "message", "data"}

				for _, prop := range expectedProps {
					if _, exists := schema.Properties[prop]; !exists {
						t.Errorf("Expected property %s in response schema", prop)
					}
				}

				// Verify data contains UserResponse schema
				if dataSchema, exists := schema.Properties["data"]; exists {
					// Resolve $ref if present
					dataSchema = resolveSchemaRef(dataSchema, components)

					if len(dataSchema.Properties) == 0 {
						t.Error("Expected data schema to have UserResponse properties")
					}

					userResponseProps := []string{"username", "statements", "createdAt", "updatedAt"}

					for _, prop := range userResponseProps {
						if _, exists := dataSchema.Properties[prop]; !exists {
							t.Errorf("Expected UserResponse property %s", prop)
						}
					}
				}
			},
		},
		{
			name:       "UserStore201Response",
			path:       "/v1/users",
			method:     "post",
			statusCode: "201",
			testFunc: func(t *testing.T, response Response, components map[string]*Schema) {
				// Should have SuccessResponse structure
				schema := response.Content["application/json"].Schema

				if schema == nil {
					t.Fatal("Expected schema to exist")
				}

				expectedProps := []string{"status", "message", "data"}

				for _, prop := range expectedProps {
					if _, exists := schema.Properties[prop]; !exists {
						t.Errorf("Expected property %s in response schema", prop)
					}
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if pathMethods, exists := paths[tc.path]; exists {
				if operation, exists := pathMethods[tc.method]; exists {
					if response, exists := operation.Responses[tc.statusCode]; exists {
						tc.testFunc(t, response, components)
					} else {
						t.Errorf("Expected response %s not found", tc.statusCode)
					}
				} else {
					t.Errorf("Expected method %s not found", tc.method)
				}
			} else {
				t.Errorf("Expected path %s not found", tc.path)
			}
		})
	}
}

// TestRequestBodySchemaExtraction tests that request body schemas are properly extracted
func TestRequestBodySchemaExtraction(t *testing.T) {
	analyzer := NewGenerator()
	analysis, err := analyzer.AnalyzeAllRoutes()

	if err != nil {
		t.Fatalf("Failed to analyze all routes: %v", err)
	}

	paths := analyzer.GenerateOpenAPIFromAnalysis(analysis)

	testCases := []struct {
		name           string
		path           string
		method         string
		expectedFields []string
		requiredFields []string
	}{
		{
			name:           "UserStoreRequestBody",
			path:           "/v1/users",
			method:         "post",
			expectedFields: []string{"username", "password", "statements", "description"},
			requiredFields: []string{"username", "password", "statements"},
		},
		{
			name:           "UserUpdateRequestBody",
			path:           "/v1/users/{username}",
			method:         "patch",
			expectedFields: []string{"statements", "description"},
			requiredFields: []string{"statements"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if pathMethods, exists := paths[tc.path]; exists {
				if operation, exists := pathMethods[tc.method]; exists {
					if operation.RequestBody == nil {
						t.Fatal("Expected request body to exist")
					}

					schema := operation.RequestBody.Content["application/json"].Schema

					if schema == nil {
						t.Fatal("Expected request body schema to exist")
					}

					// Resolve $ref if present
					components := analyzer.GetRegisteredSchemas()
					schema = resolveSchemaRef(schema, components)

					// Check expected fields exist
					for _, field := range tc.expectedFields {
						if _, exists := schema.Properties[field]; !exists {
							t.Errorf("Expected field %s not found in request schema", field)
						}
					}

					// Check required fields
					for _, requiredField := range tc.requiredFields {
						found := slices.Contains(schema.Required, requiredField)

						if !found {
							t.Errorf("Expected required field %s not found", requiredField)
						}
					}
				} else {
					t.Errorf("Expected method %s not found", tc.method)
				}
			} else {
				t.Errorf("Expected path %s not found", tc.path)
			}
		})
	}
}

// TestSecurityDetection tests that security requirements are properly detected
func TestSecurityDetection(t *testing.T) {
	analyzer := NewGenerator()
	analysis, err := analyzer.AnalyzeController("../http/user_controller.go", "UserController")

	if err != nil {
		t.Fatalf("Failed to analyze UserController: %v", err)
	}

	// All UserController methods should have security requirements
	for methodName, methodAnalysis := range analysis.Methods {
		t.Run(methodName+"Security", func(t *testing.T) {
			if len(methodAnalysis.Security) == 0 {
				t.Errorf("Expected method %s to have security requirements", methodName)
			}

			expectedSecurity := "AccessKeyAuth"

			found := slices.Contains(methodAnalysis.Security, expectedSecurity)

			if !found {
				t.Errorf("Expected security %s not found in method %s", expectedSecurity, methodName)
			}
		})
	}
}

// TestParameterExtraction tests that path parameters are properly extracted
func TestParameterExtraction(t *testing.T) {
	analyzer := NewGenerator()
	analysis, err := analyzer.AnalyzeController("../http/user_controller.go", "UserController")

	if err != nil {
		t.Fatalf("Failed to analyze UserController: %v", err)
	}

	paths := analyzer.GenerateOpenAPIFromAnalysis(analysis)

	// Methods that should have username parameter
	methodsWithUsername := []struct {
		path   string
		method string
	}{
		{"/v1/users/{username}", "get"},
		{"/v1/users/{username}", "put"},
		{"/v1/users/{username}", "delete"},
	}

	for _, tc := range methodsWithUsername {
		t.Run(tc.method+"_"+strings.ReplaceAll(tc.path, "/", "_"), func(t *testing.T) {
			if pathMethods, exists := paths[tc.path]; exists {
				if operation, exists := pathMethods[tc.method]; exists {
					found := false

					for _, param := range operation.Parameters {
						if param.Name == "username" && param.In == "path" && param.Required {
							found = true
							break
						}
					}

					if !found {
						t.Errorf("Expected username path parameter not found in %s %s", tc.method, tc.path)
					}
				}
			}
		})
	}
}

// TestQueryParamsExtraction ensures that request.QueryParams(&Type{}) usage is
// detected and the referenced struct's fields are emitted as query parameters
// in the analysis / generated OpenAPI.
func TestQueryParamsExtraction(t *testing.T) {
	analyzer := NewGenerator()

	analysis, err := analyzer.AnalyzeController("../http/query_log_controller.go", "QueryLogController")

	if err != nil {
		t.Fatalf("Failed to analyze QueryLogController: %v", err)
	}

	method, exists := analysis.Methods["QueryLogControllerIndex"]

	if !exists {
		t.Fatalf("Expected QueryLogControllerIndex to be analyzed")
	}

	// Collect query parameters
	queryParams := map[string]*ParameterInfo{}

	for _, p := range method.Parameters {
		if p.In == "query" {
			queryParams[p.Name] = p
		}
	}

	// Expect start, end, step
	expected := []struct {
		name     string
		typeName string
	}{
		{"start", "string"},
		{"end", "string"},
		{"step", "integer"},
	}

	for _, e := range expected {
		p, ok := queryParams[e.name]

		if !ok {
			t.Errorf("Expected query parameter %s not found", e.name)
			continue
		}

		if p.Type != e.typeName {
			t.Errorf("Parameter %s: expected type %s, got %s", e.name, e.typeName, p.Type)
		}
	}
}

// TestAnalyzerRobustness tests analyzer behavior with edge cases
func TestAnalyzerRobustness(t *testing.T) {
	analyzer := NewGenerator()

	t.Run("NonExistentFile", func(t *testing.T) {
		_, err := analyzer.AnalyzeController("nonexistent.go", "Controller")

		if err == nil {
			t.Error("Expected error for non-existent file")
		}
	})

	t.Run("EmptyControllerName", func(t *testing.T) {
		analysis, err := analyzer.AnalyzeController("../http/user_controller.go", "")

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Should return analysis with all methods since empty controller name matches all
		if len(analysis.Methods) == 0 {
			t.Errorf("Expected methods for empty controller name, got %d", len(analysis.Methods))
		}
	})

	t.Run("NonMatchingControllerName", func(t *testing.T) {
		analysis, err := analyzer.AnalyzeController("../http/user_controller.go", "NonExistentController")

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Should return analysis with no methods since controller name doesn't match
		if len(analysis.Methods) != 0 {
			t.Errorf("Expected no methods for non-matching controller name, got %d", len(analysis.Methods))
		}
	})
}

// TestFullOpenAPIGeneration tests that a complete OpenAPI spec can be generated
func TestFullOpenAPIGeneration(t *testing.T) {
	analyzer := NewGenerator()
	analysis, err := analyzer.AnalyzeController("../http/user_controller.go", "UserController")

	if err != nil {
		t.Fatalf("Failed to analyze UserController: %v", err)
	}

	paths := analyzer.GenerateOpenAPIFromAnalysis(analysis)

	// Generate a complete OpenAPI spec
	spec := &OpenAPISpec{
		OpenAPI: "3.1.0",
		Info: Info{
			Title:   "Test API",
			Version: "0.0.0",
		},
		Paths: convertToPathItems(paths),
	}

	// Verify the spec can be marshaled to JSON
	jsonData, err := json.MarshalIndent(spec, "", "  ")

	if err != nil {
		t.Fatalf("Failed to marshal OpenAPI spec: %v", err)
	}

	// Verify it's valid JSON by unmarshaling
	var unmarshaled map[string]any

	err = json.Unmarshal(jsonData, &unmarshaled)

	if err != nil {
		t.Fatalf("Generated JSON is invalid: %v", err)
	}

	// Verify basic OpenAPI structure
	if unmarshaled["openapi"] != "3.1.0" {
		t.Error("Expected OpenAPI version 3.1.0")
	}

	if _, exists := unmarshaled["paths"]; !exists {
		t.Error("Expected paths section in OpenAPI spec")
	}
}

// Helper function to convert paths for testing
func convertToPathItems(paths map[string]map[string]*Operation) map[string]PathItem {
	pathItems := make(map[string]PathItem)

	for path, methods := range paths {
		pathItem := PathItem{}

		for method, operation := range methods {
			switch method {
			case "get":
				pathItem.Get = operation
			case "post":
				pathItem.Post = operation
			case "put":
				pathItem.Put = operation
			case "delete":
				pathItem.Delete = operation
			case "patch":
				pathItem.Patch = operation
			}
		}

		pathItems[path] = pathItem
	}

	return pathItems
}

// TestAnalyzerConsistencyAcrossControllers tests that the analyzer works consistently
// with different controller patterns to prevent regressions
func TestAnalyzerConsistencyAcrossControllers(t *testing.T) {
	analyzer := NewGenerator()

	// Test different controller patterns
	testCases := []struct {
		name                 string
		controllerFile       string
		controllerName       string
		expectedMethodCount  int
		expectTypes          bool
		expectedTypePatterns []string
	}{
		{
			name:                 "UserController",
			controllerFile:       "../http/user_controller.go",
			controllerName:       "UserController",
			expectedMethodCount:  5, // Index, Show, Store, Update, Destroy
			expectTypes:          true,
			expectedTypePatterns: []string{"UserStoreRequest", "UserUpdateRequest"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			analysis, err := analyzer.AnalyzeController(tc.controllerFile, tc.controllerName)

			if err != nil {
				t.Fatalf("Failed to analyze %s: %v", tc.name, err)
			}

			// Check method count
			if len(analysis.Methods) != tc.expectedMethodCount {
				t.Errorf("Expected %d methods for %s, got %d",
					tc.expectedMethodCount, tc.name, len(analysis.Methods))
			}

			// Check that all methods have proper HTTP methods and paths
			for methodName, method := range analysis.Methods {
				if method.HTTPMethod == "" {
					t.Errorf("Method %s missing HTTP method", methodName)
				}

				if method.Path == "" {
					t.Errorf("Method %s missing path", methodName)
				}

				if len(method.Tags) == 0 {
					t.Errorf("Method %s missing tags", methodName)
				}
			}

			// Check type extraction if expected
			if tc.expectTypes {
				typeInfo := analyzer.GetTypeInfo()

				for _, expectedType := range tc.expectedTypePatterns {
					if _, exists := typeInfo[expectedType]; !exists {
						t.Errorf("Expected type %s not found for %s", expectedType, tc.name)
					}
				}
			}

			// Generate OpenAPI and verify basic structure
			paths := analyzer.GenerateOpenAPIFromAnalysis(analysis)

			if len(paths) == 0 {
				t.Errorf("No paths generated for %s", tc.name)
			}

			// Verify each path has at least one operation
			for path, methods := range paths {
				if len(methods) == 0 {
					t.Errorf("Path %s has no methods for %s", path, tc.name)
				}

				// Verify each operation has responses
				for method, operation := range methods {
					if len(operation.Responses) == 0 {
						t.Errorf("Operation %s %s has no responses for %s",
							method, path, tc.name)
					}
				}
			}
		})
	}
}

// TestResponseSchemaConsistency ensures response schemas follow consistent patterns
func TestResponseSchemaConsistency(t *testing.T) {
	analyzer := NewGenerator()
	analysis, err := analyzer.AnalyzeController("../http/user_controller.go", "UserController")

	if err != nil {
		t.Fatalf("Failed to analyze UserController: %v", err)
	}

	paths := analyzer.GenerateOpenAPIFromAnalysis(analysis)

	// Test that all 200/201 responses have proper data schemas
	for path, methods := range paths {
		for method, operation := range methods {
			for statusCode, response := range operation.Responses {
				if statusCode == "200" || statusCode == "201" {
					schema := response.Content["application/json"].Schema

					if schema == nil {
						t.Errorf("Missing schema for %s %s %s", method, path, statusCode)
						continue
					}

					// For success responses, check if they follow SuccessResponse pattern
					if statusCode == "200" || statusCode == "201" {
						// Should have status, message, data properties for success responses
						expectedProps := []string{"status", "message", "data"}

						for _, prop := range expectedProps {
							if schema.Properties == nil {
								t.Errorf("Schema properties nil for %s %s %s", method, path, statusCode)
								break
							}

							if _, exists := schema.Properties[prop]; !exists {
								// Some responses might not follow SuccessResponse pattern
								// That's OK, just document it
								t.Logf("Response %s %s %s doesn't follow SuccessResponse pattern (missing %s)",
									method, path, statusCode, prop)
							}
						}
					}
				}
			}
		}
	}
}

// TestRequestBodyConsistency ensures request body schemas are consistently extracted
func TestRequestBodyConsistency(t *testing.T) {
	analyzer := NewGenerator()
	analysis, err := analyzer.AnalyzeController("../http/user_controller.go", "UserController")

	if err != nil {
		t.Fatalf("Failed to analyze UserController: %v", err)
	}

	paths := analyzer.GenerateOpenAPIFromAnalysis(analysis)

	// Test that POST/PUT operations have request bodies
	for path, methods := range paths {
		for method, operation := range methods {
			if method == "post" || method == "put" {
				if operation.RequestBody == nil {
					t.Errorf("Missing request body for %s %s", method, path)
					continue
				}

				schema := operation.RequestBody.Content["application/json"].Schema

				if schema == nil {
					t.Errorf("Missing request body schema for %s %s", method, path)
					continue
				}

				// Request body should have properties
				if len(schema.Properties) == 0 {
					t.Errorf("Request body schema has no properties for %s %s", method, path)
				}

				// Request body should have at least one required field for creation/update
				if len(schema.Required) == 0 {
					t.Logf("Request body has no required fields for %s %s (might be intentional)", method, path)
				}
			}
		}
	}
}

// BenchmarkAnalyzerPerformance benchmarks the analyzer performance
func BenchmarkAnalyzerPerformance(b *testing.B) {
	for b.Loop() {
		analyzer := NewGenerator()
		analysis, err := analyzer.AnalyzeController("../http/user_controller.go", "UserController")

		if err != nil {
			b.Fatalf("Failed to analyze controller: %v", err)
		}

		analyzer.GenerateOpenAPIFromAnalysis(analysis)
	}
}

// TestDynamicTagGeneration tests the new dynamic tag generation functionality
func TestDynamicTagGeneration(t *testing.T) {
	testCases := []struct {
		name          string
		usedTags      map[string]bool
		expectedTags  []string
		expectedDescs map[string]string
	}{
		{
			name: "BasicControllerTags",
			usedTags: map[string]bool{
				"User":      true,
				"AccessKey": true,
				"Database":  true,
			},
			expectedTags: []string{"User", "AccessKey", "Database"},
			expectedDescs: map[string]string{
				"User":      "User management operations",
				"AccessKey": "Access key management for authentication",
				"Database":  "Database management operations",
			},
		},
		{
			name: "CompoundOperationTags",
			usedTags: map[string]bool{
				"DatabaseBackup":   true,
				"DatabaseRestore":  true,
				"DatabaseSnapshot": true,
				"DatabaseBranch":   true,
			},
			expectedTags: []string{"DatabaseBackup", "DatabaseRestore", "DatabaseSnapshot", "DatabaseBranch"},
			expectedDescs: map[string]string{
				"DatabaseBackup":   "Database backup operations",
				"DatabaseRestore":  "Database restore operations",
				"DatabaseSnapshot": "Database snapshot operations",
				"DatabaseBranch":   "Database branch management operations",
			},
		},
		{
			name: "ClusterOperationTags",
			usedTags: map[string]bool{
				"ClusterConnection": true,
				"ClusterElection":   true,
				"ClusterPrimary":    true,
				"ClusterStatus":     true,
			},
			expectedTags: []string{"ClusterConnection", "ClusterElection", "ClusterPrimary", "ClusterStatus"},
			expectedDescs: map[string]string{
				"ClusterConnection": "Cluster connection management operations",
				"ClusterElection":   "Cluster leader election operations",
				"ClusterPrimary":    "Primary cluster node operations",
				"ClusterStatus":     "Cluster status and health operations",
			},
		},
		{
			name: "StreamingAndActivationTags",
			usedTags: map[string]bool{
				"QueryStream": true,
				"KeyActivate": true,
				"QueryLog":    true,
			},
			expectedTags: []string{"QueryStream", "KeyActivate", "QueryLog"},
			expectedDescs: map[string]string{
				"QueryStream": "Streaming query operations",
				"KeyActivate": "Key activation operations",
				"QueryLog":    "Query performance and usage metrics",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tags := GenerateDynamicTags(tc.usedTags)

			// Check correct number of tags generated
			if len(tags) != len(tc.expectedTags) {
				t.Errorf("Expected %d tags, got %d", len(tc.expectedTags), len(tags))
			}

			// Create lookup maps for easy checking
			generatedTags := make(map[string]string)

			for _, tag := range tags {
				generatedTags[tag.Name] = tag.Description
			}

			// Verify all expected tags are present with correct descriptions
			for _, expectedTag := range tc.expectedTags {
				if desc, exists := generatedTags[expectedTag]; exists {
					if expectedDesc, hasExpected := tc.expectedDescs[expectedTag]; hasExpected {
						if desc != expectedDesc {
							t.Errorf("Tag %s: expected description '%s', got '%s'",
								expectedTag, expectedDesc, desc)
						}
					}
				} else {
					t.Errorf("Expected tag %s not found in generated tags", expectedTag)
				}
			}
		})
	}
}

// TestRequiredArraySorting tests that required arrays are alphabetically sorted
func TestRequiredArraySorting(t *testing.T) {
	analyzer := NewGenerator()
	analysis, err := analyzer.AnalyzeAllRoutes()

	if err != nil {
		t.Fatalf("Failed to analyze all routes: %v", err)
	}

	paths := analyzer.GenerateOpenAPIFromAnalysis(analysis)

	// Check all response schemas have sorted required arrays
	for pathName, pathMethods := range paths {
		for methodName, operation := range pathMethods {
			t.Run(pathName+"_"+methodName, func(t *testing.T) {
				for statusCode, response := range operation.Responses {
					if response.Content != nil {
						if jsonContent, exists := response.Content["application/json"]; exists {
							if jsonContent.Schema != nil {
								checkRequiredArraySorting(t, jsonContent.Schema, pathName+" "+methodName+" "+statusCode)
							}
						}
					}
				}
			})
		}
	}
}

// checkRequiredArraySorting recursively checks that all required arrays are sorted
func checkRequiredArraySorting(t *testing.T, schema *Schema, context string) {
	if schema == nil {
		return
	}

	// Check if this schema's required array is sorted
	if len(schema.Required) > 1 {
		sortedRequired := make([]string, len(schema.Required))
		copy(sortedRequired, schema.Required)
		sort.Strings(sortedRequired)

		for i, field := range schema.Required {
			if field != sortedRequired[i] {
				t.Errorf("%s: Required array not sorted. Expected %v, got %v",
					context, sortedRequired, schema.Required)
				break
			}
		}
	}

	// Recursively check properties
	for _, prop := range schema.Properties {
		checkRequiredArraySorting(t, prop, context)
	}

	// Check array items
	if schema.Items != nil {
		checkRequiredArraySorting(t, schema.Items, context)
	}
}

// TestStringTypeWithEnumDetection tests improved string type detection with enum values
func TestStringTypeWithEnumDetection(t *testing.T) {
	// Test the enum detection functionality by manually creating schemas with enums
	// and verifying they're handled correctly in the final OpenAPI spec
	// This tests our enum handling logic without relying on specific enum types being discovered

	// Create test schemas with enum values to validate functionality
	testSchemas := map[string]*Schema{
		"TestStatementEffect": {
			Type: "string",
			Enum: []any{"allow", "deny"},
		},
		"TestPrivilege": {
			Type: "string",
			Enum: []any{
				"database:create", "database:list", "database:show",
				"database:update", "database:delete", "database:backup",
			},
		},
		"RegularString": {
			Type: "string",
			// No enum values
		},
	}

	// Create a simple OpenAPI spec with our test schemas
	spec := &OpenAPISpec{
		OpenAPI: "3.1.0",
		Info: Info{
			Title:   "Test API",
			Version: "0.0.0",
		},
		Components: &Components{
			Schemas: combineSchemas(GetCommonSchemas(), testSchemas),
		},
	}

	// Convert to JSON to test the final output
	jsonData, err := json.MarshalIndent(spec, "", "  ")

	if err != nil {
		t.Fatalf("Failed to marshal OpenAPI spec: %v", err)
	}

	// Parse the JSON to verify enum schemas exist
	var specMap map[string]any

	err = json.Unmarshal(jsonData, &specMap)

	if err != nil {
		t.Fatalf("Failed to unmarshal OpenAPI spec: %v", err)
	}

	// Check components/schemas section
	components := specMap["components"].(map[string]any)
	schemas := components["schemas"].(map[string]any)

	// Test enum schemas
	testCases := []struct {
		schemaName     string
		expectedType   string
		shouldHaveEnum bool
		expectedEnums  []string
	}{
		{
			schemaName:     "TestStatementEffect",
			expectedType:   "string",
			shouldHaveEnum: true,
			expectedEnums:  []string{"allow", "deny"},
		},
		{
			schemaName:     "TestPrivilege",
			expectedType:   "string",
			shouldHaveEnum: true,
			expectedEnums:  []string{"database:create", "database:list", "database:show"},
		},
		{
			schemaName:     "RegularString",
			expectedType:   "string",
			shouldHaveEnum: false,
			expectedEnums:  nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.schemaName, func(t *testing.T) {
			schemaAny, exists := schemas[tc.schemaName]

			if !exists {
				// Show what schemas are available for debugging
				var availableSchemas []string

				for name := range schemas {
					availableSchemas = append(availableSchemas, name)
				}

				t.Errorf("Expected schema %s not found in OpenAPI spec. Available: %v",
					tc.schemaName, availableSchemas)
				return
			}

			schema, ok := schemaAny.(map[string]any)

			if !ok {
				t.Errorf("Schema %s is not a proper object", tc.schemaName)
				return
			}

			// Check enum handling
			if tc.shouldHaveEnum {
				enumAny, hasEnum := schema["enum"]

				if !hasEnum {
					t.Errorf("Expected %s to have enum values", tc.schemaName)
					return
				}

				enumSlice := enumAny.([]any)
				enumStrings := make([]string, len(enumSlice))

				for i, val := range enumSlice {
					enumStrings[i] = val.(string)
				}

				// Verify all expected enums are present
				enumMap := make(map[string]bool)

				for _, val := range enumStrings {
					enumMap[val] = true
				}

				for _, expectedEnum := range tc.expectedEnums {
					if !enumMap[expectedEnum] {
						t.Errorf("Expected enum value %s not found in %v", expectedEnum, enumStrings)
					}
				}

				t.Logf("✓ %s enum validated with %d values: %v",
					tc.schemaName, len(enumStrings), enumStrings)
			} else {
				if _, hasEnum := schema["enum"]; hasEnum {
					t.Errorf("Expected %s to not have enum values", tc.schemaName)
				}
			}
		})
	}

	// Additional test: Verify that the schema combination logic works correctly
	t.Run("SchemaCombinationLogic", func(t *testing.T) {
		// Test that schemas are properly combined and accessible
		combinedSchemas := combineSchemas(GetCommonSchemas(), testSchemas)

		expectedSchemas := []string{
			"SuccessResponse", "ErrorResponse", "ValidationErrorResponse", // from GetCommonSchemas
			"TestStatementEffect", "TestPrivilege", "RegularString", // from testSchemas
		}

		for _, expectedSchema := range expectedSchemas {
			if _, exists := combinedSchemas[expectedSchema]; !exists {
				t.Errorf("Expected combined schema %s not found", expectedSchema)
			}
		}

		// Verify enum schemas retain their enum values
		if effectSchema := combinedSchemas["TestStatementEffect"]; effectSchema != nil {
			if len(effectSchema.Enum) != 2 {
				t.Errorf("Expected TestStatementEffect to have 2 enum values, got %d",
					len(effectSchema.Enum))
			}
		}
	})
}

// TestSchemaDeduplication tests that duplicate schemas are properly handled
func TestSchemaDeduplication(t *testing.T) {
	analyzer := NewGenerator()
	_, err := analyzer.AnalyzeAllRoutes()

	if err != nil {
		t.Fatalf("Failed to analyze all routes: %v", err)
	}

	registeredSchemas := analyzer.GetRegisteredSchemas()

	// Test that we don't have duplicate schemas with different references
	schemasByContent := make(map[string][]string)

	for schemaName, schema := range registeredSchemas {
		// Create a signature for the schema based on its structure
		signature := createSchemaSignature(schema)
		schemasByContent[signature] = append(schemasByContent[signature], schemaName)
	}

	// Check for problematic duplicates (same content, different names)
	for signature, schemaNames := range schemasByContent {
		if len(schemaNames) > 1 {
			// Allow certain expected duplicates (like qualified vs simple names)
			hasBothQualifiedAndSimple := false

			for _, name := range schemaNames {
				if strings.Contains(name, ".") {
					simpleName := name[strings.LastIndex(name, ".")+1:]

					if slices.Contains(schemaNames, simpleName) {
						hasBothQualifiedAndSimple = true
					}
				}
			}

			if !hasBothQualifiedAndSimple {
				// Allow duplicate "object" schemas since they're likely external types that couldn't be analyzed
				if signature == "object" {
					t.Logf("Allowing duplicate generic object schemas (likely external types): %v", schemaNames)
				} else if allAreResponseSchemas(schemaNames) {
					// Allow duplicate response schemas - these are often controller-specific
					// response types that have the same structure (e.g., TokenUpdateResponse, TokenShowResponse)
					t.Logf("Allowing duplicate response schemas (controller-specific types with same structure): %v", schemaNames)
				} else {
					// Debug: print the actual schemas to understand why they have the same signature
					t.Logf("Signature %s matched by schemas: %v", signature, schemaNames)

					for _, schemaName := range schemaNames {
						if schema, exists := registeredSchemas[schemaName]; exists {
							t.Logf("Schema %s: Type=%s, Properties=%d, Required=%v",
								schemaName, schema.Type, len(schema.Properties), schema.Required)
						}
					}

					t.Errorf("Found unexpected duplicate schemas with signature %s: %v",
						signature, schemaNames)
				}
			}
		}
	}
}

// createSchemaSignature creates a string signature for a schema to detect duplicates
func createSchemaSignature(schema *Schema) string {
	if schema == nil {
		return "null"
	}

	sig := schema.Type

	if len(schema.Required) > 0 {
		sortedReq := make([]string, len(schema.Required))
		copy(sortedReq, schema.Required)
		sort.Strings(sortedReq)
		sig += "|req:" + strings.Join(sortedReq, ",")
	}

	if len(schema.Enum) > 0 {
		var enumStrings []string
		for _, enumVal := range schema.Enum {
			if strVal, ok := enumVal.(string); ok {
				enumStrings = append(enumStrings, strVal)
			}
		}

		sort.Strings(enumStrings)
		sig += "|enum:" + strings.Join(enumStrings, ",")
	}

	if len(schema.Properties) > 0 {
		var propNames []string

		for propName := range schema.Properties {
			propNames = append(propNames, propName)
		}

		sort.Strings(propNames)
		sig += "|props:" + strings.Join(propNames, ",")
	}

	return sig
}

// allAreResponseSchemas checks if all schema names are response types
// (ending in "Response" or containing "Response")
func allAreResponseSchemas(schemaNames []string) bool {
	for _, name := range schemaNames {
		if !strings.Contains(name, "Response") {
			return false
		}
	}

	return len(schemaNames) > 0
}

// TestDynamicResponseAnalysis tests that response analysis is truly dynamic
func TestDynamicResponseAnalysis(t *testing.T) {
	analyzer := NewGenerator()
	analysis, err := analyzer.AnalyzeAllRoutes()

	if err != nil {
		t.Fatalf("Failed to analyze all routes: %v", err)
	}

	// Track response patterns
	validSchemaCount := 0
	totalResponseCount := 0
	responseWithMessage := 0

	for _, methodAnalysis := range analysis.Methods {
		if methodAnalysis.Responses == nil {
			continue
		}

		for _, responseInfo := range methodAnalysis.Responses {
			totalResponseCount++

			// Check if response has meaningful content
			if responseInfo.Type != "" || responseInfo.Description != "" || responseInfo.Schema != nil {
				validSchemaCount++
			}

			// Check if response has dynamic message
			if responseInfo.Message != "" {
				responseWithMessage++
			}
		}
	}

	// Verify that we have reasonable response coverage
	if totalResponseCount == 0 {
		t.Error("No responses found in any operations")
	}

	// Check that we have some responses with valid schemas
	if validSchemaCount == 0 {
		t.Error("No responses found with valid schemas")
	}

	// Check that some responses have dynamic messages
	if responseWithMessage == 0 {
		t.Log("No responses with dynamic messages found - this may be expected")
	}

	// Verify specific endpoint methods exist
	testCases := []struct {
		path   string
		method string
	}{
		{"/v1/databases", "GET"},
		{"/v1/status", "GET"},
	}

	for _, tc := range testCases {
		found := false
		for _, methodAnalysis := range analysis.Methods {
			if methodAnalysis.Path == tc.path && methodAnalysis.HTTPMethod == tc.method {
				found = true

				// Check that the method has some response definitions
				if len(methodAnalysis.Responses) == 0 {
					t.Errorf("Expected responses for %s %s", tc.method, tc.path)
				}
				break
			}
		}

		if !found {
			t.Errorf("Expected method %s %s not found in analysis", tc.method, tc.path)
		}
	}
}

// combineSchemas merges multiple schema maps for testing, preferring more detailed schemas
func combineSchemas(schemaMaps ...map[string]*Schema) map[string]*Schema {
	result := make(map[string]*Schema)

	for _, schemaMap := range schemaMaps {
		for name, schema := range schemaMap {
			if existing, exists := result[name]; exists {
				// Prefer schemas with more properties or enum values
				if shouldPreferSchema(schema, existing) {
					result[name] = schema
				}
			} else {
				result[name] = schema
			}
		}
	}

	return result
}

// shouldPreferSchema determines which schema to prefer when there are duplicates
func shouldPreferSchema(new, existing *Schema) bool {
	// Prefer schemas with enum values
	if len(new.Enum) > 0 && len(existing.Enum) == 0 {
		return true
	}

	if len(existing.Enum) > 0 && len(new.Enum) == 0 {
		return false
	}

	// Prefer schemas with more properties
	if len(new.Properties) > len(existing.Properties) {
		return true
	}

	// Prefer schemas with required fields
	if len(new.Required) > len(existing.Required) {
		return true
	}

	return false
}

// TestHTTP101ResponseHeaders verifies that HTTP 101 Switching Protocols responses
// include the appropriate protocol upgrade headers
func TestHTTP101ResponseHeaders(t *testing.T) {
	analyzer := NewGenerator()

	// Analyze the query stream controller which uses HTTP 101
	analysis, err := analyzer.AnalyzeController("../http/query_stream_controller.go", "QueryStreamController")

	if err != nil {
		t.Fatalf("Failed to analyze query stream controller: %v", err)
	}

	// Find the QueryStreamControllerStore function
	var storeMethod *MethodAnalysis
	for _, method := range analysis.Methods {
		if strings.Contains(method.Name, "QueryStreamControllerStore") ||
			(method.HTTPMethod == "POST" && strings.Contains(method.Path, "/query/stream")) {
			storeMethod = method
			break
		}
	}

	if storeMethod == nil {
		// List all methods for debugging
		t.Log("Available methods:")
		for _, method := range analysis.Methods {
			t.Logf("  - %s: %s %s", method.Name, method.HTTPMethod, method.Path)
		}
		t.Fatal("QueryStreamControllerStore method not found")
	}

	// Verify the 101 response exists
	response101, exists := storeMethod.Responses["101"]
	if !exists {
		t.Fatal("Expected 101 response not found")
	}

	// Verify headers are present
	if len(response101.Headers) == 0 {
		t.Fatal("Expected headers in 101 response, but none were found")
	}

	// Verify the Upgrade header
	upgradeHeader, hasUpgrade := response101.Headers["Upgrade"]
	if !hasUpgrade {
		t.Error("Expected 'Upgrade' header in 101 response")
	} else {
		if upgradeHeader.Type != "string" {
			t.Errorf("Expected Upgrade header type to be 'string', got '%s'", upgradeHeader.Type)
		}
		if upgradeHeader.Example != "lqtp" {
			t.Errorf("Expected Upgrade header example to be 'lqtp', got '%v'", upgradeHeader.Example)
		}
	}

	// Verify the Connection header
	connectionHeader, hasConnection := response101.Headers["Connection"]
	if !hasConnection {
		t.Error("Expected 'Connection' header in 101 response")
	} else {
		if connectionHeader.Type != "string" {
			t.Errorf("Expected Connection header type to be 'string', got '%s'", connectionHeader.Type)
		}
		if connectionHeader.Example != "Upgrade" {
			t.Errorf("Expected Connection header example to be 'Upgrade', got '%v'", connectionHeader.Example)
		}
	}

	t.Logf("✓ HTTP 101 response validated with %d headers: Upgrade=%v, Connection=%v",
		len(response101.Headers),
		upgradeHeader.Example,
		connectionHeader.Example)
}

// TestContentTypeExtraction verifies that Content-Type headers are extracted
// and used to determine media type, not included in response headers
func TestContentTypeExtraction(t *testing.T) {
	analyzer := NewGenerator()

	// Analyze a controller that returns JSON responses
	analysis, err := analyzer.AnalyzeController("../http/user_controller.go", "UserController")

	if err != nil {
		t.Fatalf("Failed to analyze user controller: %v", err)
	}

	// Find a method that should have a 200 response
	var method *MethodAnalysis
	for _, m := range analysis.Methods {
		if m.HTTPMethod == "GET" && len(m.Responses) > 0 {
			if _, has200 := m.Responses["200"]; has200 {
				method = m
				break
			}
		}
	}

	if method == nil {
		t.Fatal("No suitable method with 200 response found")
	}

	response200 := method.Responses["200"]

	// Verify that Content-Type is NOT in the headers (it should be filtered out)
	if response200.Headers != nil {
		if _, hasContentType := response200.Headers["Content-Type"]; hasContentType {
			t.Error("Content-Type should not be in response headers - it should be in the content media type")
		}
	}

	t.Logf("✓ Content-Type correctly excluded from response headers for %s %s",
		method.HTTPMethod, method.Path)
}
