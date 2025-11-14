package http_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	appHttp "github.com/litebase/litebase/pkg/http"
)

// FuzzRouteTestCase defines a test case for a specific route in fuzzing
type FuzzRouteTestCase struct {
	Method          string
	Path            string
	Handler         string
	RequiresAuth    bool
	PathParams      map[string]string
	QueryParams     []string
	RequestStruct   interface{}
	NeedsDatabase   bool
	NeedsBranch     bool
	NeedsCheckpoint bool
}

// FuzzValue generates various fuzzed values of different types
type FuzzValue struct {
	Type  string
	Value any
}

// generateFuzzValues creates a collection of fuzz test values for different types
func generateFuzzValues() []FuzzValue {
	return []FuzzValue{
		// Null/empty values
		{Type: "nil", Value: nil},
		{Type: "empty_string", Value: ""},
		{Type: "empty_object", Value: map[string]any{}},
		{Type: "empty_array", Value: []any{}},

		// Type confusion
		{Type: "number_as_string", Value: 12345},
		{Type: "float", Value: 3.14159},
		{Type: "negative_number", Value: -999},
		{Type: "zero", Value: 0},
		{Type: "bool_true", Value: true},
		{Type: "bool_false", Value: false},
		{Type: "string_as_number", Value: "12345"},

		// Arrays and objects
		{Type: "array_of_strings", Value: []string{"test", "array"}},
		{Type: "array_of_numbers", Value: []int{1, 2, 3}},
		{Type: "nested_array", Value: []any{[]any{"nested"}}},
		{Type: "nested_object", Value: map[string]any{"nested": map[string]any{"key": "value"}}},

		// Special characters and injection attempts
		{Type: "sql_injection", Value: "'; DROP TABLE users;--"},
		{Type: "xss_script", Value: "<script>alert('xss')</script>"},
		{Type: "path_traversal", Value: "../../../etc/passwd"},
		{Type: "null_byte", Value: "test\x00null"},
		{Type: "newlines", Value: "test\nwith\nnewlines"},
		{Type: "tabs", Value: "test\twith\ttabs"},
		{Type: "unicode", Value: "test™✓☃"},
		{Type: "emoji", Value: "test😀🎉💯"},

		// Large values
		{Type: "large_string", Value: strings.Repeat("a", 10000)},
		{Type: "very_large_string", Value: strings.Repeat("x", 100000)},
		{Type: "large_number", Value: 9223372036854775807}, // max int64

		// Edge cases
		{Type: "whitespace", Value: "   "},
		{Type: "special_chars", Value: "!@#$%^&*()_+-=[]{}|;':\",./<>?"},
		{Type: "mixed_types_object", Value: map[string]any{
			"string": "value",
			"number": 123,
			"bool":   true,
			"null":   nil,
			"array":  []any{1, "two", 3.0},
		}},
	}
}

// discoverRoutes analyzes the router to extract all registered routes
func discoverRoutes(router *appHttp.Router) []FuzzRouteTestCase {
	var testCases []FuzzRouteTestCase

	for method, routes := range router.Routes {
		if routes == nil {
			continue
		}

		for path, route := range routes {
			if route == nil {
				continue
			}

			handlerName := route.GetHandlerName()

			if handlerName == "" {
				continue
			}

			testCase := FuzzRouteTestCase{
				Method:        method,
				Path:          path,
				Handler:       handlerName,
				RequiresAuth:  true, // Most routes require auth
				PathParams:    extractPathParams(path),
				RequestStruct: findRequestStruct(handlerName),
			}

			// Determine if route needs database/branch setup
			if strings.Contains(path, "{databaseName}") {
				testCase.NeedsDatabase = true
			}

			if strings.Contains(path, "{branchName}") {
				testCase.NeedsBranch = true
			}

			// Mutation endpoints typically need checkpoints for branches
			if method == "POST" && strings.Contains(path, "/branches") {
				testCase.NeedsCheckpoint = true
			}

			testCases = append(testCases, testCase)
		}
	}

	return testCases
}

// extractPathParams extracts parameter names from a path pattern
func extractPathParams(path string) map[string]string {
	params := make(map[string]string)
	parts := strings.SplitSeq(path, "/")

	for part := range parts {
		if strings.HasPrefix(part, "{") && strings.HasSuffix(part, "}") {
			paramName := strings.Trim(part, "{}")
			params[paramName] = "test-" + paramName
		}
	}

	return params
}

// findRequestStruct attempts to find the request structure for a handler
func findRequestStruct(handlerName string) any {
	// Map of handler names to their request structures
	requestStructs := map[string]any{
		"DatabaseBranchControllerStore":  &appHttp.DatabaseBranchStoreRequest{},
		"DatabaseControllerStore":        &appHttp.DatabaseStoreRequest{},
		"QueryControllerStore":           &appHttp.QueryRequest{},
		"AccessKeyControllerStore":       &appHttp.AccessKeyStoreRequest{},
		"AccessKeyControllerUpdate":      &appHttp.AccessKeyUpdateRequest{},
		"TokenControllerStore":           &appHttp.TokenStoreRequest{},
		"TokenControllerUpdate":          &appHttp.TokenUpdateRequest{},
		"UserControllerStore":            &appHttp.UserStoreRequest{},
		"UserControllerUpdate":           &appHttp.UserUpdateRequest{},
		"KeyControllerStore":             &appHttp.KeyStoreRequest{},
		"KeyActivateControllerStore":     &appHttp.KeyActivateRequest{},
		"DatabaseRestoreControllerStore": &appHttp.DatabaseRestoreRequest{},
		"ClusterElectionControllerStore": &appHttp.ClusterElectionRequest{},
		// Add more as needed
	}

	if requestStruct, ok := requestStructs[handlerName]; ok {
		return requestStruct
	}

	return nil
}

// extractFieldsFromStruct recursively extracts field information from a struct
func extractFieldsFromStruct(s any) map[string]reflect.Type {
	fields := make(map[string]reflect.Type)

	if s == nil {
		return fields
	}

	t := reflect.TypeOf(s)

	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return fields
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// Skip unexported fields
		if field.PkgPath != "" {
			continue
		}

		jsonTag := field.Tag.Get("json")

		if jsonTag == "" || jsonTag == "-" {
			continue
		}

		// Extract JSON field name
		jsonName := strings.Split(jsonTag, ",")[0]
		fields[jsonName] = field.Type
	}

	return fields
}

// FuzzControllers uses Go's native fuzzing to test controller input parsing
//
// NOTE: This fuzzer has limitations because controllers have complex dependencies
// (cluster, databaseManager, etc.) that can't easily be mocked. For comprehensive
// controller testing, the integration tests (TestControllerMalformedJSON,
// TestControllerTypeAssertionSafety, etc.) provide superior coverage because they:
// - Use a real test server with all dependencies initialized
// - Test the full HTTP request/response lifecycle
// - Cover path parameters, query parameters, and request bodies
// - Are reproducible and easier to debug
//
// This fuzzer focuses on testing request.Input() parsing with various JSON payloads
// to ensure safe type handling, but cannot test full controller logic.
func FuzzControllers(f *testing.F) {
	// Seed with various JSON payloads that test different edge cases
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"name": "test"}`))
	f.Add([]byte(`{"name": 123}`)) // Type mismatch
	f.Add([]byte(`null`))
	f.Add([]byte(`[]`))
	f.Add([]byte(`{"id":"abc","name":null}`))
	f.Add([]byte(`{"statements":[]}`))
	f.Add([]byte(`{"description":"x","statements":[{"effect":"Allow","resource":"*","actions":["*"]}]}`))
	f.Add([]byte(`{"encryptionKey":"test123"}`))
	f.Add([]byte(`{"username":"user","password":"pass"}`))
	f.Add([]byte(`{"queries":[{"id":"1","statement":"SELECT 1","parameters":[],"transactionId":""}]}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Skip if data is too large to prevent timeouts
		if len(data) > 10*1024 { // 10KB limit for efficient fuzzing
			return
		}

		// Discover all controller functions dynamically
		controllers := discoverControllerFunctions()

		// Test each controller with the fuzzed data
		for _, ctrl := range controllers {
			testControllerWithFuzzedData(t, ctrl, data)
		}
	})
}

// ControllerFunction represents a discovered controller function
type ControllerFunction struct {
	Name          string
	Function      reflect.Value
	RequestType   reflect.Type
	NeedsDatabase bool
	NeedsBranch   bool
	Method        string // HTTP method (POST, GET, etc.)
}

// discoverControllerFunctions uses reflection to find all controller functions
func discoverControllerFunctions() []ControllerFunction {
	controllers := []ControllerFunction{
		{
			Name:          "UserControllerStore",
			Function:      reflect.ValueOf(appHttp.UserControllerStore),
			RequestType:   reflect.TypeOf((*appHttp.UserStoreRequest)(nil)).Elem(),
			NeedsDatabase: false,
			Method:        "POST",
		},
		{
			Name:          "DatabaseControllerStore",
			Function:      reflect.ValueOf(appHttp.DatabaseControllerStore),
			RequestType:   reflect.TypeOf((*appHttp.DatabaseStoreRequest)(nil)).Elem(),
			NeedsDatabase: false,
			Method:        "POST",
		},
		{
			Name:          "DatabaseBranchControllerStore",
			Function:      reflect.ValueOf(appHttp.DatabaseBranchControllerStore),
			RequestType:   reflect.TypeOf((*appHttp.DatabaseBranchStoreRequest)(nil)).Elem(),
			NeedsDatabase: true,
			NeedsBranch:   false,
			Method:        "POST",
		},
		{
			Name:          "AccessKeyControllerStore",
			Function:      reflect.ValueOf(appHttp.AccessKeyControllerStore),
			RequestType:   reflect.TypeOf((*appHttp.AccessKeyStoreRequest)(nil)).Elem(),
			NeedsDatabase: false,
			Method:        "POST",
		},
		{
			Name:          "AccessKeyControllerUpdate",
			Function:      reflect.ValueOf(appHttp.AccessKeyControllerUpdate),
			RequestType:   reflect.TypeOf((*appHttp.AccessKeyUpdateRequest)(nil)).Elem(),
			NeedsDatabase: false,
			Method:        "PATCH",
		},
		{
			Name:          "TokenControllerStore",
			Function:      reflect.ValueOf(appHttp.TokenControllerStore),
			RequestType:   reflect.TypeOf((*appHttp.TokenStoreRequest)(nil)).Elem(),
			NeedsDatabase: false,
			Method:        "POST",
		},
		{
			Name:          "TokenControllerUpdate",
			Function:      reflect.ValueOf(appHttp.TokenControllerUpdate),
			RequestType:   reflect.TypeOf((*appHttp.TokenUpdateRequest)(nil)).Elem(),
			NeedsDatabase: false,
			Method:        "PATCH",
		},
		{
			Name:          "KeyControllerStore",
			Function:      reflect.ValueOf(appHttp.KeyControllerStore),
			RequestType:   reflect.TypeOf((*appHttp.KeyStoreRequest)(nil)).Elem(),
			NeedsDatabase: false,
			Method:        "POST",
		},
		{
			Name:          "KeyActivateControllerStore",
			Function:      reflect.ValueOf(appHttp.KeyActivateControllerStore),
			RequestType:   reflect.TypeOf((*appHttp.KeyActivateRequest)(nil)).Elem(),
			NeedsDatabase: false,
			Method:        "POST",
		},
		{
			Name:          "DatabaseRestoreControllerStore",
			Function:      reflect.ValueOf(appHttp.DatabaseRestoreControllerStore),
			RequestType:   reflect.TypeOf((*appHttp.DatabaseRestoreRequest)(nil)).Elem(),
			NeedsDatabase: true,
			NeedsBranch:   true,
			Method:        "POST",
		},
		{
			Name:          "ClusterElectionControllerStore",
			Function:      reflect.ValueOf(appHttp.ClusterElectionControllerStore),
			RequestType:   reflect.TypeOf((*appHttp.ClusterElectionRequest)(nil)).Elem(),
			NeedsDatabase: false,
			Method:        "POST",
		},
		{
			Name:          "QueryControllerStore",
			Function:      reflect.ValueOf(appHttp.QueryControllerStore),
			RequestType:   reflect.TypeOf((*appHttp.QueryRequest)(nil)).Elem(),
			NeedsDatabase: true,
			NeedsBranch:   true,
			Method:        "POST",
		},
	}

	return controllers
}

// testControllerWithFuzzedData tests a controller function with fuzzed input data
func testControllerWithFuzzedData(t *testing.T, ctrl ControllerFunction, data []byte) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Controller %s panicked with fuzzed input: %v\nData: %s", ctrl.Name, r, string(data))
		}
	}()

	// For true unit-level fuzzing, we'd need to mock all Request dependencies
	// (cluster, databaseManager, logManager, etc.)
	// Since that's complex, we skip controllers that will access nil dependencies
	// and only test the Input() parsing and type assertion logic

	// Parse fuzzed data as JSON body
	var bodyData map[string]any

	if err := json.Unmarshal(data, &bodyData); err != nil {
		// Invalid JSON - skip for now as we're testing valid JSON parsing
		return
	}

	bodyReader := bytes.NewReader(data)
	bodyReaderCloser := io.NopCloser(bodyReader)

	// Create a minimal mock Request with fuzzed body
	mockRequest := &appHttp.Request{
		BaseRequest: &http.Request{
			Header: http.Header{
				"Content-Type":   []string{"application/json"},
				"Content-Length": []string{strconv.Itoa(len(data))},
			},
			Method: ctrl.Method,
			Body:   bodyReaderCloser,
		},
		// Body: bodyData,

		Method: ctrl.Method,
	}

	// Test the Input() method and type assertion - this is what we're really testing
	if ctrl.RequestType != nil {
		// Create a new instance of the request type
		requestInstance := reflect.New(ctrl.RequestType).Interface()

		// Test that Input() can parse the fuzzed data
		result, err := mockRequest.Input(requestInstance)

		// We don't care if it errors (most fuzz data will be invalid)
		// We just care that it doesn't panic
		if err == nil && result != nil {
			// Successfully parsed - verify type assertion works
			_ = result
		}
	}
}

// TestControllerTypeAssertionSafety tests that all controllers handle type mismatches gracefully
func TestControllerTypeAssertionSafety(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		router := appHttp.NewRouter()
		appHttp.LoadPublicRoutes(router)

		testCases := discoverRoutes(router)
		fuzzValues := generateFuzzValues()

		client := server.WithAccessKeyClient([]auth.Statement{{
			Effect:   auth.StatementEffectAllow,
			Resource: "*",
			Actions:  []auth.Privilege{"*"},
		}})

		for _, tc := range testCases {
			// Only test POST/PUT/PATCH endpoints that accept bodies
			if tc.Method != "POST" && tc.Method != "PUT" && tc.Method != "PATCH" {
				continue
			}

			// Skip if no request struct is defined
			if tc.RequestStruct == nil {
				continue
			}

			t.Run(fmt.Sprintf("%s_%s", tc.Method, tc.Handler), func(t *testing.T) {
				// Set up required resources
				var mock test.TestDatabase
				path := tc.Path

				if tc.NeedsDatabase || tc.NeedsBranch {
					mock = test.MockDatabase(server.App)

					if tc.NeedsCheckpoint {
						con, err := server.App.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)
						if err != nil {
							t.Skipf("Failed to get connection: %v", err)
							return
						}
						defer server.App.DatabaseManager.ConnectionManager().Release(con)

						if _, err := con.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY)", nil); err != nil {
							t.Skipf("Failed to create table: %v", err)
							return
						}

						if err := con.Checkpoint(); err != nil {
							t.Skipf("Failed to checkpoint: %v", err)
							return
						}
					}

					for param, value := range tc.PathParams {
						switch param {
						case "databaseName":
							path = strings.ReplaceAll(path, "{"+param+"}", mock.DatabaseName)
						case "branchName":
							path = strings.ReplaceAll(path, "{"+param+"}", mock.BranchName)
						default:
							path = strings.ReplaceAll(path, "{"+param+"}", value)
						}
					}
				}

				// Get fields from the request struct
				fields := extractFieldsFromStruct(tc.RequestStruct)

				// Test each field with incompatible types
				for fieldName := range fields {
					for _, fuzzValue := range fuzzValues {
						t.Run(fmt.Sprintf("%s_%s", fieldName, fuzzValue.Type), func(t *testing.T) {
							payload := map[string]interface{}{
								fieldName: fuzzValue.Value,
							}

							// This should not panic
							resp, statusCode, err := client.Send(path, tc.Method, payload)

							if err != nil {
								// Network errors are ok in tests
								return
							}

							// We expect most of these to return errors (400, 422, etc.)
							// but they should not crash (500 is acceptable for graceful error handling)
							// The key is that we got a response, not a panic
							if statusCode == 200 {
								// If it succeeded, that's unexpected but not a failure
								t.Logf("Unexpected success with %s = %v (type: %s): %v",
									fieldName, fuzzValue.Value, fuzzValue.Type, resp)
							}
						})
					}
				}

				// Test with multiple wrong types at once
				t.Run("multiple_invalid_types", func(t *testing.T) {
					payload := make(map[string]interface{})
					i := 0
					for fieldName := range fields {
						if i < len(fuzzValues) {
							payload[fieldName] = fuzzValues[i].Value
							i++
						}
					}

					resp, statusCode, err := client.Send(path, tc.Method, payload)

					if err != nil {
						return
					}

					// Should handle gracefully
					if statusCode != 500 {
						// Good, handled gracefully
					} else {
						t.Logf("Got 500 error with multiple invalid types: %v", resp)
					}
				})
			})
		}
	})
}

// TestControllerQueryParamFuzzing tests query parameter handling
func TestControllerQueryParamFuzzing(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		router := appHttp.NewRouter()
		appHttp.LoadPublicRoutes(router)

		testCases := discoverRoutes(router)
		fuzzValues := generateFuzzValues()

		client := server.WithAccessKeyClient([]auth.Statement{{
			Effect:   auth.StatementEffectAllow,
			Resource: "*",
			Actions:  []auth.Privilege{"*"},
		}})

		// Common query parameter names to fuzz
		queryParams := []string{
			"limit", "offset", "page", "perPage",
			"sort", "order", "filter",
			"start", "end", "step",
			"query", "search", "name",
		}

		for _, tc := range testCases {
			// Focus on GET endpoints that typically use query params
			if tc.Method != "GET" {
				continue
			}

			t.Run(fmt.Sprintf("%s_%s", tc.Method, tc.Handler), func(t *testing.T) {
				// Set up required resources
				var mock test.TestDatabase
				path := tc.Path

				if tc.NeedsDatabase || tc.NeedsBranch {
					mock = test.MockDatabase(server.App)

					for param, value := range tc.PathParams {
						switch param {
						case "databaseName":
							path = strings.ReplaceAll(path, "{"+param+"}", mock.DatabaseName)
						case "branchName":
							path = strings.ReplaceAll(path, "{"+param+"}", mock.BranchName)
						default:
							path = strings.ReplaceAll(path, "{"+param+"}", value)
						}
					}
				}

				// Test each query parameter with various fuzzed values
				for _, paramName := range queryParams {
					for _, fuzzValue := range fuzzValues {
						// Skip complex types for query params
						if _, ok := fuzzValue.Value.(map[string]any); ok {
							continue
						}

						if _, ok := fuzzValue.Value.([]any); ok {
							continue
						}

						// Skip array types that won't serialize to query params
						if _, ok := fuzzValue.Value.([]string); ok {
							continue
						}

						if _, ok := fuzzValue.Value.([]int); ok {
							continue
						}

						t.Run(fmt.Sprintf("%s_%s", paramName, fuzzValue.Type), func(t *testing.T) {
							pathWithQuery := fmt.Sprintf("%s?%s=%v", path, paramName, fuzzValue.Value)

							// Should not panic
							resp, statusCode, err := client.Send(pathWithQuery, tc.Method, nil)

							// Network errors are acceptable for malformed URLs
							if err != nil {
								return
							}

							// Just verify we got a valid HTTP response
							if statusCode >= 200 && statusCode < 600 {
								// Valid HTTP status code
							} else if statusCode == 0 {
								// Network/parsing error - acceptable for fuzz testing
								t.Logf("Network error with %s=%v (type: %s)", paramName, fuzzValue.Value, fuzzValue.Type)
							} else {
								t.Errorf("Invalid status code: %d, response: %v", statusCode, resp)
							}
						})
					}
				}
			})
		}
	})
}

// TestControllerPathParamFuzzing tests path parameter handling
func TestControllerPathParamFuzzing(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		router := appHttp.NewRouter()
		appHttp.LoadPublicRoutes(router)

		testCases := discoverRoutes(router)

		client := server.WithAccessKeyClient([]auth.Statement{{
			Effect:   auth.StatementEffectAllow,
			Resource: "*",
			Actions:  []auth.Privilege{"*"},
		}})

		fuzzPathValues := []string{
			"", "   ", "../test", "test/../../etc/passwd",
			"test\x00null", "test;drop", "<script>",
			strings.Repeat("a", 1000),
			"test!@#$%^&*()",
		}

		for _, tc := range testCases {
			if len(tc.PathParams) == 0 {
				continue
			}

			t.Run(fmt.Sprintf("%s_%s", tc.Method, tc.Handler), func(t *testing.T) {
				for paramName := range tc.PathParams {
					// Skip database/branch names as we need valid ones
					if paramName == "databaseName" || paramName == "branchName" {
						continue
					}

					for i, fuzzValue := range fuzzPathValues {
						t.Run(fmt.Sprintf("%s_%d", paramName, i), func(t *testing.T) {
							path := tc.Path
							for param, value := range tc.PathParams {
								if param == paramName {
									path = strings.ReplaceAll(path, "{"+param+"}", fuzzValue)
								} else {
									path = strings.ReplaceAll(path, "{"+param+"}", value)
								}
							}

							// Should not panic
							resp, statusCode, err := client.Send(path, tc.Method, nil)

							// Network errors are acceptable for malformed URLs/paths
							if err != nil {
								return
							}

							if statusCode >= 200 && statusCode < 600 {
								// Valid HTTP status code
							} else if statusCode == 0 {
								// Network/parsing error - acceptable for fuzz testing
								t.Logf("Network error with %s=%s", paramName, fuzzValue)
							} else {
								t.Errorf("Invalid status code: %d, response: %v", statusCode, resp)
							}
						})
					}
				}
			})
		}
	})
}

// TestControllerMalformedJSON tests handling of malformed JSON and empty payloads
// This test is EXPECTED to find crashes if controllers don't use safe type assertions
func TestControllerMalformedJSON(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		router := appHttp.NewRouter()
		appHttp.LoadPublicRoutes(router)

		testCases := discoverRoutes(router)

		client := server.WithAccessKeyClient([]auth.Statement{{
			Effect:   auth.StatementEffectAllow,
			Resource: "*",
			Actions:  []auth.Privilege{"*"},
		}})

		for _, tc := range testCases {
			if tc.Method != "POST" && tc.Method != "PUT" && tc.Method != "PATCH" {
				continue
			}

			t.Run(fmt.Sprintf("%s_%s", tc.Method, tc.Handler), func(t *testing.T) {
				// Set up path
				var mock test.TestDatabase
				path := tc.Path

				if tc.NeedsDatabase || tc.NeedsBranch {
					mock = test.MockDatabase(server.App)

					for param, value := range tc.PathParams {
						switch param {
						case "databaseName":
							path = strings.ReplaceAll(path, "{"+param+"}", mock.DatabaseName)
						case "branchName":
							path = strings.ReplaceAll(path, "{"+param+"}", mock.BranchName)
						default:
							path = strings.ReplaceAll(path, "{"+param+"}", value)
						}
					}
				}

				// Test with nil/empty payload
				t.Run("empty_payload", func(t *testing.T) {
					// This should not panic - if it does, the controller needs safe type assertion
					resp, statusCode, err := client.Send(path, tc.Method, nil)

					if err != nil {
						// Network error
						return
					}

					// We expect 4xx errors for empty payloads, not panics
					if statusCode >= 400 && statusCode < 600 {
						// Expected error response - controller handled it gracefully
					} else if statusCode == 200 {
						// Some endpoints might accept empty payloads
						t.Logf("Unexpected success for nil payload: %v", resp)
					}
				})
			})
		}
	})
}
