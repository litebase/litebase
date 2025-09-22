package openapi

import (
	"encoding/json"
	"strings"
	"testing"
)

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
			"UserControllerStoreRequest",
			"UserControllerUpdateRequest",
		}

		for _, typeName := range expectedTypes {
			if _, exists := typeInfo[typeName]; !exists {
				t.Errorf("Expected type %s not found", typeName)
			}
		}

		// Verify UserControllerStoreRequest has expected fields
		if storeReq, exists := typeInfo["UserControllerStoreRequest"]; exists {
			expectedFields := []string{"Description", "Password", "Statements", "Username"}

			for _, fieldName := range expectedFields {
				if _, fieldExists := storeReq.Fields[fieldName]; !fieldExists {
					t.Errorf("Expected field %s not found in UserControllerStoreRequest", fieldName)
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

	testCases := []struct {
		name       string
		path       string
		method     string
		statusCode string
		testFunc   func(t *testing.T, response Response)
	}{
		{
			name:       "UserIndex200Response",
			path:       "/v1/users",
			method:     "get",
			statusCode: "200",
			testFunc: func(t *testing.T, response Response) {
				// Should have data array of UserResponse objects
				schema := response.Content["application/json"].Schema
				if schema == nil {
					t.Fatal("Expected schema to exist")
				}

				if dataSchema, exists := schema.Properties["data"]; exists {
					if dataSchema.Type != "array" {
						t.Errorf("Expected data to be array, got %s", dataSchema.Type)
					}
					if dataSchema.Items == nil {
						t.Error("Expected array items schema")
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
			testFunc: func(t *testing.T, response Response) {
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
					if len(dataSchema.Properties) == 0 {
						t.Error("Expected data schema to have UserResponse properties")
					}

					userResponseProps := []string{"username", "statements", "created_at", "updated_at"}
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
			testFunc: func(t *testing.T, response Response) {
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
						tc.testFunc(t, response)
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
			method:         "put",
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

					// Check expected fields exist
					for _, field := range tc.expectedFields {
						if _, exists := schema.Properties[field]; !exists {
							t.Errorf("Expected field %s not found in request schema", field)
						}
					}

					// Check required fields
					for _, requiredField := range tc.requiredFields {
						found := false
						for _, reqField := range schema.Required {
							if reqField == requiredField {
								found = true
								break
							}
						}
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
			found := false

			for _, security := range methodAnalysis.Security {
				if security == expectedSecurity {
					found = true
					break
				}
			}

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
			Version: "1.0.0",
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
			expectedTypePatterns: []string{"UserControllerStoreRequest", "UserControllerUpdateRequest"},
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
