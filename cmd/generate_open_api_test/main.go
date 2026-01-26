package main

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/litebase/litebase/pkg/auth"
)

// Request represents a single HTTP request in a test case
type Request struct {
	Name         string         `json:"name"`
	Model        string         `json:"model"`                  // e.g. "AccessKey"
	Operation    string         `json:"operation"`              // e.g. "createAccessKey"
	Body         map[string]any `json:"body,omitempty"`         // Model payload
	RequestModel string         `json:"requestModel,omitempty"` // e.g. TokenStoreRequest (component schema name)
	Parameters   []string       `json:"parameters,omitempty"`   // Array of parameter names that reference captured values
}

// Response represents the expected response for a request
type Response struct {
	StatusCode   int            `json:"statusCode"`
	Content      map[string]any `json:"content,omitempty"`
	Captures     []string       `json:"captures,omitempty"`     // Array of field names to capture from response. Supports nested objects using dot notation (e.g., "restorePoint.timestamp")
	IsBinaryFile bool           `json:"isBinaryFile,omitempty"` // true if the response is a binary file (application/octet-stream) instead of JSON
}

// WaitStep represents a wait/delay instruction in a test
type WaitStep struct {
	Name     string `json:"name"`
	Duration int    `json:"duration"` // Duration in milliseconds
	Reason   string `json:"reason"`
}

// TestStep represents a single request-response pair in a test, or a wait instruction
type TestStep struct {
	Request  *Request  `json:"request,omitempty"`
	Response *Response `json:"response,omitempty"`
	Wait     *WaitStep `json:"wait,omitempty"`
}

// TestCase represents a complete test scenario (may have multiple steps)
type TestCase struct {
	OperationID string     `json:"operationId"`
	Name        string     `json:"name"`
	Description string     `json:"description,omitempty"`
	Steps       []TestStep `json:"steps"`
}

// ApiTests represents tests grouped by API class (e.g., AccessKeyApi)
type ApiTests struct {
	Tests map[string]TestCase `json:"tests"` // Map of operation_id to test case
}

// TestSuite represents the entire test suite structure
type TestSuite struct {
	BeforeAll  []TestStep          `json:"beforeAll,omitempty"`
	BeforeEach []TestStep          `json:"beforeEach,omitempty"`
	Apis       map[string]ApiTests `json:"apis"` // Map of tag to API tests
}

// Global registry of operations that exist in the OpenAPI spec
var availableOperations = make(map[string]bool)

// Global reference to OpenAPI paths for parameter lookup
var openAPIPaths map[string]any

// Operations to skip during test generation
var skipOperations = map[string]string{
	"createQueryStream": "WebSocket streaming not testable in standard SDK test suites",
}

// buildOperationRegistry scans the OpenAPI spec and builds a registry of all available operations
func buildOperationRegistry(paths map[string]any) {
	for _, methods := range paths {
		methodMap, ok := methods.(map[string]any)

		if !ok {
			continue
		}

		for method, details := range methodMap {
			if method == "parameters" {
				continue
			}

			detailMap, ok := details.(map[string]any)

			if !ok {
				continue
			}

			if operationID, ok := detailMap["operationId"].(string); ok {
				availableOperations[operationID] = true
			}
		}
	}

	fmt.Printf("Found %d operations in OpenAPI spec\n", len(availableOperations))
}

// operationExists checks if an operation ID exists in the OpenAPI spec
func operationExists(operationID string) bool {
	return availableOperations[operationID]
}

// shouldSkipOperation checks if an operation should be skipped during test generation
func shouldSkipOperation(operationID string) bool {
	_, skip := skipOperations[operationID]

	return skip
}

func main() {
	// Load OpenAPI spec
	filePath := "./api/generated_open_api.json"
	file, err := os.ReadFile(filePath)

	if err != nil {
		fmt.Printf("Error reading OpenAPI spec: %v\n", err)
		os.Exit(1)
	}

	var openAPISpec map[string]any

	if err := json.Unmarshal(file, &openAPISpec); err != nil {
		fmt.Printf("Error parsing OpenAPI spec: %v\n", err)
		os.Exit(1)
	}

	// Extract paths
	paths, ok := openAPISpec["paths"].(map[string]any)

	if !ok {
		fmt.Println("Invalid OpenAPI spec: missing paths")
		os.Exit(1)
	}

	// Set global paths for parameter lookup
	openAPIPaths = paths

	// Build registry of available operations
	buildOperationRegistry(paths)

	// Build test suite
	testSuite := TestSuite{
		BeforeAll:  generateBeforeAll(),
		BeforeEach: generateBeforeEach(),
		Apis:       make(map[string]ApiTests),
	}

	// Generate test cases for each operation, grouped by tag
	for path, methods := range paths {
		methodMap, ok := methods.(map[string]any)

		if !ok {
			continue
		}

		// Extract path-level parameters (shared across all methods)
		var pathLevelParams []any

		if params, ok := methodMap["parameters"].([]any); ok {
			pathLevelParams = params
		}

		for method, details := range methodMap {
			if method == "parameters" {
				continue // Skip parameter definitions
			}

			detailMap, ok := details.(map[string]any)

			if !ok {
				continue
			}

			// Merge path-level parameters into operation details
			if len(pathLevelParams) > 0 {
				detailMapCopy := make(map[string]any)
				maps.Copy(detailMapCopy, detailMap)
				detailMapCopy["pathParameters"] = pathLevelParams
				detailMap = detailMapCopy
			}

			method = strings.ToUpper(method)
			operationID := extractOperationID(detailMap, method, path)

			// Skip operations that are in the exclusion list
			if shouldSkipOperation(operationID) {
				fmt.Printf("Skipping operation %s: %s\n", operationID, skipOperations[operationID])
				continue
			}

			// Extract tag (API class name)
			tags := extractTags(detailMap)

			if len(tags) == 0 {
				continue // Skip operations without tags
			}

			tag := tags[0] // Use first tag as API class name

			// Initialize API class if not exists
			if _, exists := testSuite.Apis[tag]; !exists {
				testSuite.Apis[tag] = ApiTests{
					Tests: make(map[string]TestCase),
				}
			}

			// Generate test cases based on operation type
			testCases := generateTestCasesForOperation(operationID, method, detailMap)

			for _, tc := range testCases {
				api := testSuite.Apis[tag]
				api.Tests[tc.OperationID] = tc
				testSuite.Apis[tag] = api
			}
		}
	}

	// Populate RequestModel for any requests by resolving the operation's requestBody $ref
	for apiKey, apiTests := range testSuite.Apis {
		for testKey, tc := range apiTests.Tests {
			for i, step := range tc.Steps {
				if step.Request != nil && step.Request.Operation != "" {
					// resolve by operation id
					if step.Request.RequestModel == "" {
						step.Request.RequestModel = getRequestModelForOperationID(step.Request.Operation)
						tc.Steps[i] = step
					}
				}
			}

			apiTests.Tests[testKey] = tc
		}

		testSuite.Apis[apiKey] = apiTests
	}

	// Write to JSON file
	outputFile := "./api/generated_open_api_test_cases.json"
	output, err := json.MarshalIndent(testSuite, "", "  ")

	if err != nil {
		fmt.Printf("Error generating JSON: %v\n", err)
		os.Exit(1)
	}

	if err := os.WriteFile(outputFile, output, 0644); err != nil {
		fmt.Printf("Error writing JSON file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Test cases generated successfully: %s\n", outputFile)
}

// generateBeforeAll creates setup steps to run before all tests
func generateBeforeAll() []TestStep {
	return []TestStep{
		{
			Request: &Request{
				Name:      "Create test token",
				Model:     "Token",
				Operation: "createToken",
				Body: map[string]any{
					"description": "test_token",
					"statements": []map[string]any{
						{
							"effect":   "allow",
							"resource": "*",
							"actions":  []auth.Privilege{"*"},
						},
					},
				},
			},
			Response: &Response{
				StatusCode: 201,
				Content:    map[string]any{},
				Captures:   []string{"token"},
			},
		},
	}
}

// generateBeforeEach creates steps to run before each test
func generateBeforeEach() []TestStep {
	return []TestStep{}
}

// extractOperationID gets the operation ID from the OpenAPI spec
func extractOperationID(detailMap map[string]any, method, path string) string {
	if op, ok := detailMap["operationId"].(string); ok {
		return op
	}

	return generateOperationID(method, path)
}

// generateOperationID builds a deterministic operation id from method and path
func generateOperationID(method, path string) string {
	s := strings.ToLower(method) + "_" + path
	s = strings.ReplaceAll(s, "{", "_")
	s = strings.ReplaceAll(s, "}", "_")
	s = strings.ReplaceAll(s, "/", "_")

	for strings.Contains(s, "__") {
		s = strings.ReplaceAll(s, "__", "_")
	}

	s = strings.Trim(s, "_")

	return s
}

// generateTestCasesForOperation generates test cases based on operation type and dependencies
func generateTestCasesForOperation(operationID, method string, details map[string]any) []TestCase {
	var testCases []TestCase

	// Extract tags to determine resource type
	tags := extractTags(details)

	resourceType := ""

	if len(tags) > 0 {
		resourceType = tags[0]
	}

	// Determine operation type (list, create, get, update, delete)
	operationType := determineOperationType(operationID, method)

	switch operationType {
	case "list":
		testCases = append(testCases, generateListTestCase(operationID, resourceType, details))
	case "create":
		testCases = append(testCases, generateCreateTestCase(operationID, resourceType, details))
	case "get":
		testCases = append(testCases, generateGetTestCase(operationID, resourceType, details))
	case "update":
		testCases = append(testCases, generateUpdateTestCase(operationID, resourceType, details))
	case "delete":
		testCases = append(testCases, generateDeleteTestCase(operationID, resourceType, details))
	default:
		// Generic test case for other operations
		testCases = append(testCases, generateGenericTestCase(operationID, resourceType))
	}

	return testCases
}

// getOperationParameters returns the path parameters required for a specific operation from the OpenAPI spec
func getOperationParameters(operationID string) []string {
	// Find the operation in the OpenAPI spec
	for _, methods := range openAPIPaths {
		methodMap, ok := methods.(map[string]any)

		if !ok {
			continue
		}

		// Extract path-level parameters
		var pathLevelParams []any

		if params, ok := methodMap["parameters"].([]any); ok {
			pathLevelParams = params
		}

		for method, details := range methodMap {
			if method == "parameters" {
				continue
			}

			detailMap, ok := details.(map[string]any)

			if !ok {
				continue
			}

			if opID, ok := detailMap["operationId"].(string); ok && opID == operationID {
				// Found the operation - merge path-level parameters
				if len(pathLevelParams) > 0 {
					detailMapCopy := make(map[string]any)
					maps.Copy(detailMapCopy, detailMap)
					detailMapCopy["pathParameters"] = pathLevelParams
					detailMap = detailMapCopy
				}

				// Extract its path parameters
				return extractPathParameters(detailMap)
			}
		}
	}

	return []string{}
}

// extractTags extracts tags from operation details
func extractTags(details map[string]any) []string {
	var tags []string

	if tagsArray, ok := details["tags"].([]any); ok {
		for _, tag := range tagsArray {
			if tagStr, ok := tag.(string); ok {
				tags = append(tags, tagStr)
			}
		}
	}

	return tags
}

// extractPathParameters extracts path parameters from the operation details or parent path object
func extractPathParameters(details map[string]any) []string {
	var params []string

	// Check path-level parameters first (stored in pathParameters by the main loop)
	if pathParams, ok := details["pathParameters"].([]any); ok {
		for _, param := range pathParams {
			if paramMap, ok := param.(map[string]any); ok {
				if name, ok := paramMap["name"].(string); ok {
					if in, ok := paramMap["in"].(string); ok && in == "path" {
						params = append(params, name)
					}
				}
			}
		}
	}

	// Check operation-level parameters (can override or add to path-level)
	if paramsArray, ok := details["parameters"].([]any); ok {
		for _, param := range paramsArray {
			if paramMap, ok := param.(map[string]any); ok {
				if name, ok := paramMap["name"].(string); ok {
					if in, ok := paramMap["in"].(string); ok && in == "path" {
						// Only add if not already in the list
						found := slices.Contains(params, name)

						if !found {
							params = append(params, name)
						}
					}
				}
			}
		}
	}

	return params
}

// isReadOnlyResource checks if a resource type only has read operations (list/get) and cannot be created via API
func isReadOnlyResource(resourceType string) bool {
	readOnlyResources := map[string]bool{
		"ClusterStatus": true, // Only has listClusterStatuses
	}

	return readOnlyResources[resourceType]
}

// getPluralResourceName returns the proper plural form of a resource type for display
func getPluralResourceName(resourceType string) string {
	pluralMap := map[string]string{
		"DatabaseBranch":   "DatabaseBranches",
		"DatabaseBackup":   "DatabaseBackups",
		"DatabaseSnapshot": "DatabaseSnapshots",
		"ClusterStatus":    "ClusterStatuses",
		"QueryLog":         "QueryLogs",
		"ErrorLog":         "ErrorLogs",
		"AccessKey":        "AccessKeys",
		"Database":         "Databases",
		"Token":            "Tokens",
		"User":             "Users",
	}

	if plural, ok := pluralMap[resourceType]; ok {
		return plural
	}

	return resourceType + "s"
}

// getListOperationID generates the correct list operation ID (plural form)
func getListOperationID(resourceType string) string {
	// Map resource types to their plural list operation IDs
	pluralMap := map[string]string{
		"AccessKey":        "listAccessKeys",
		"Database":         "listDatabases",
		"DatabaseBranch":   "listDatabaseBranches",
		"DatabaseBackup":   "listDatabaseBackups",
		"DatabaseSnapshot": "listDatabaseSnapshots",
		"Token":            "listTokens",
		"User":             "listUsers",
		"QueryLog":         "listQueryLogs",
		"ErrorLog":         "listErrorLogs",
		"ClusterStatus":    "listClusterStatuses",
		"Node":             "listNodes",
		"Cluster":          "listClusters",
	}

	// Return the mapped plural form, or default to "list" + resourceType + "s"
	if plural, ok := pluralMap[resourceType]; ok {
		return plural
	}

	return "list" + resourceType + "s"
}

// determineOperationType determines the CRUD operation type
func determineOperationType(operationID, method string) string {
	lowerID := strings.ToLower(operationID)

	if strings.HasPrefix(lowerID, "list") {
		return "list"
	}

	if strings.HasPrefix(lowerID, "create") {
		return "create"
	}

	if strings.HasPrefix(lowerID, "get") && method == "GET" {
		return "get"
	}

	if strings.HasPrefix(lowerID, "update") || strings.HasPrefix(lowerID, "put") {
		return "update"
	}

	if strings.HasPrefix(lowerID, "delete") {
		return "delete"
	}

	return "other"
}

// generateListTestCase generates a test for list operations
func generateListTestCase(operationID, resourceType string, details map[string]any) TestCase {
	steps := []TestStep{}

	// Check if this is a read-only resource (no create operation)
	if isReadOnlyResource(resourceType) {
		// For read-only resources, just test the list operation directly
		steps = append(steps, TestStep{
			Request: &Request{
				Name:       fmt.Sprintf("List %s", getPluralResourceName(resourceType)),
				Model:      resourceType,
				Operation:  operationID,
				Body:       map[string]any{},
				Parameters: extractPathParameters(details),
			},
			Response: &Response{
				StatusCode: 200,
				Content: map[string]any{
					"data": []any{},
				},
			},
		})

		return TestCase{
			OperationID: operationID,
			Name:        fmt.Sprintf("Test %s - List resources", operationID),
			Description: fmt.Sprintf("Verifies the %s list operation works (read-only resource)", resourceType),
			Steps:       steps,
		}
	}

	// For resources with create operations, first create a resource, then verify it's in the list
	createOp := getCreateOperationID(resourceType)

	// Special handling for DatabaseBackup - needs database, branch, write, and wait
	if resourceType == "DatabaseBackup" {
		steps = append(steps, generateDatabaseBackupPrerequisites()...)
		// DatabaseBackup prerequisites already include the create step, so skip the normal create
	} else if resourceType == "DatabaseBranch" {
		// Special handling for DatabaseBranch - needs database, write, and wait
		steps = append(steps, generateDatabaseBranchPrerequisites()...)
	} else if resourceType == "DatabaseSnapshot" {
		// Special handling for DatabaseSnapshot - needs database, branch, write, and wait (no create operation)
		steps = append(steps, generateDatabaseSnapshotPrerequisites()...)
	} else if resourceType == "QueryLog" {
		steps = append(steps, generateQueryLogPrerequisites()...)

		steps = append(steps, TestStep{
			Request: &Request{
				Name:       fmt.Sprintf("List %s", getPluralResourceName(resourceType)),
				Model:      resourceType,
				Operation:  operationID,
				Body:       map[string]any{},
				Parameters: extractPathParameters(details),
			},
			Response: &Response{
				StatusCode: 200,
				Content: map[string]any{
					"data": []any{},
				},
			},
		})

		return TestCase{
			OperationID: operationID,
			Name:        fmt.Sprintf("Test %s - List resources", operationID),
			Description: fmt.Sprintf("Verifies the %s list operation works (read-only resource)", resourceType),
			Steps:       steps,
		}
	} else if resourceType == "ErrorLog" {
		steps = append(steps, generateErrorLogPrerequisites()...)

		steps = append(steps, TestStep{
			Request: &Request{
				Name:       fmt.Sprintf("List %s", getPluralResourceName(resourceType)),
				Model:      resourceType,
				Operation:  operationID,
				Body:       map[string]any{},
				Parameters: extractPathParameters(details),
			},
			Response: &Response{
				StatusCode: 200,
				Content: map[string]any{
					"data": []any{},
				},
			},
		})

		return TestCase{
			OperationID: operationID,
			Name:        fmt.Sprintf("Test %s - List resources", operationID),
			Description: fmt.Sprintf("Verifies the %s list operation works (read-only resource)", resourceType),
			Steps:       steps,
		}
	} else if createOp != "" && operationExists(createOp) {
		// Step 1: Create a test resource (only if the create operation exists)
		steps = append(steps, TestStep{
			Request: &Request{
				Name:       fmt.Sprintf("Create test %s", resourceType),
				Model:      resourceType,
				Operation:  createOp,
				Body:       generateRequestBody(resourceType, "create"),
				Parameters: getOperationParameters(createOp),
			},
			Response: &Response{
				StatusCode: 201,
				Content:    map[string]any{},
				Captures:   generateCapturesForCreate(resourceType),
			},
		})
	}

	// Step 2: List resources and verify
	steps = append(steps, TestStep{
		Request: &Request{
			Name:       fmt.Sprintf("List %s", getPluralResourceName(resourceType)),
			Model:      resourceType,
			Operation:  operationID,
			Body:       map[string]any{},
			Parameters: extractPathParameters(details),
		},
		Response: &Response{
			StatusCode: 200,
			Content: map[string]any{
				"data": []any{},
			},
		},
	})

	return TestCase{
		OperationID: operationID,
		Name:        fmt.Sprintf("Test %s - List and verify created resource", operationID),
		Description: fmt.Sprintf("Creates a %s and verifies it appears in the list", resourceType),
		Steps:       steps,
	}
}

// generateCreateTestCase generates a test for create operations
func generateCreateTestCase(operationID, resourceType string, details map[string]any) TestCase {
	steps := []TestStep{}

	// Check if this resource requires parent resources to be created first
	pathParams := extractPathParameters(details)

	// Check if the resource

	// Special handling for DatabaseBackup - needs full setup with write and wait
	if resourceType == "DatabaseBackup" {
		steps = append(steps, generateDatabaseBackupPrerequisites()...)

		// Final step: Create the actual resource
		steps = append(steps, TestStep{
			Request: &Request{
				Name:       fmt.Sprintf("Create %s", resourceType),
				Model:      resourceType,
				Operation:  operationID,
				Body:       generateRequestBody(resourceType, "create"),
				Parameters: buildParametersFromPath(details),
			},
			Response: &Response{
				StatusCode: 201,
				Content:    map[string]any{},
				Captures:   generateCapturesForCreate(resourceType),
			},
		})

		return TestCase{
			OperationID: operationID,
			Name:        fmt.Sprintf("Test %s - Create resource", operationID),
			Description: fmt.Sprintf("Creates a new %s and verifies the response", resourceType),
			Steps:       steps,
		}
	}

	if resourceType == "DatabaseRestore" {
		steps = append(steps, generateDatabaseRestorePrerequisites()...)

		// Final step: Create the actual resource
		steps = append(steps, TestStep{
			Request: &Request{
				Name:       fmt.Sprintf("Create %s", resourceType),
				Model:      resourceType,
				Operation:  operationID,
				Body:       generateRequestBody(resourceType, "create"),
				Parameters: buildParametersFromPath(details),
			},
			Response: &Response{
				StatusCode: 201,
				Content:    map[string]any{},
				Captures:   generateCapturesForCreate(resourceType),
			},
		})

		return TestCase{
			OperationID: operationID,
			Name:        fmt.Sprintf("Test %s - Create resource", operationID),
			Description: fmt.Sprintf("Creates a new %s and verifies the response", resourceType),
			Steps:       steps,
		}
	}

	// If the resource needs a databaseName parameter, create a database first
	needsDatabase := false
	needsBranch := false

	for _, param := range pathParams {
		if param == "databaseName" {
			needsDatabase = true
		}

		if param == "branchName" {
			needsBranch = true
		}
	}

	// Step 1: Create parent database if needed
	if needsDatabase && resourceType != "Database" && resourceType != "DatabaseExport" {
		steps = append(steps, TestStep{
			Request: &Request{
				Name:      "Create test Database",
				Model:     "Database",
				Operation: "createDatabase",
				Body: map[string]any{
					"name": generateRandomDatabaseName(),
				},
			},
			Response: &Response{
				StatusCode: 201,
				Content:    map[string]any{},
				Captures:   []string{"databaseName", "branchName"},
			},
		})
	}

	// Step 2: Create parent branch if needed (and not creating a branch itself)
	if needsBranch && resourceType != "DatabaseBranch" && resourceType != "DatabaseExport" {
		// Write data to the database to ensure branch can be created from a non-empty state
		steps = append(steps, TestStep{
			Request: &Request{
				Name:      "Write data to database before creating branch",
				Model:     "Query",
				Operation: "createQuery",
				Body: map[string]any{
					"queries": []any{
						map[string]any{
							"id":         fmt.Sprintf("query-%d", time.Now().UTC().Nanosecond()),
							"statement":  "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO test_table (value) VALUES ('test');",
							"parameters": map[string]any{},
						},
					},
				},
				Parameters: []string{"databaseName", "branchName"},
			},
			Response: &Response{
				StatusCode: 200,
				Content:    map[string]any{},
			},
		})

		// Wait for checkpoint to ensure data is written
		steps = append(steps, TestStep{
			Wait: &WaitStep{
				Name:     "Wait for database checkpoint",
				Duration: 1000, // DatabaseCheckpointThreshold = 1 second
				Reason:   "Database branches are created from the latest checkpoint, which occurs every 1 second after writes",
			},
		})

		// Now create the branch
		steps = append(steps, TestStep{
			Request: &Request{
				Name:      "Create test DatabaseBranch",
				Model:     "DatabaseBranch",
				Operation: "createDatabaseBranch",
				Body: map[string]any{
					"name": "test-branch",
				},
				Parameters: []string{"databaseName"},
			},
			Response: &Response{
				StatusCode: 201,
				Content:    map[string]any{},
				Captures:   []string{"databaseName", "name AS branchName"},
			},
		})
	}

	// Step 3: when creating a database branch, we need to write to the database first and wait
	if resourceType == "DatabaseBranch" {
		// Write data to the database to ensure branch can be created from a non-empty state
		steps = append(steps, TestStep{
			Request: &Request{
				Name:      "Write data to database before creating branch",
				Model:     "Query",
				Operation: "createQuery",
				Body: map[string]any{
					"queries": []any{
						map[string]any{
							"id":         fmt.Sprintf("query-%d", time.Now().UTC().Nanosecond()),
							"statement":  "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO test_table (value) VALUES ('test');",
							"parameters": map[string]any{},
						},
					},
				},
				Parameters: []string{"databaseName", "branchName"},
			},
			Response: &Response{
				StatusCode: 200,
				Content:    map[string]any{},
			},
		})

		// Wait for checkpoint to ensure data is written
		steps = append(steps, TestStep{
			Wait: &WaitStep{
				Name:     "Wait for database checkpoint",
				Duration: 1000, // DatabaseCheckpointThreshold = 1 second
				Reason:   "Database branches are created from the latest checkpoint, which occurs every 1 second after writes",
			},
		})
	}

	// Step 4: when creating a DatabaseExport, we need database and data first
	if resourceType == "DatabaseExport" {
		// Create database
		steps = append(steps, TestStep{
			Request: &Request{
				Name:      "Create test Database",
				Model:     "Database",
				Operation: "createDatabase",
				Body: map[string]any{
					"name": generateRandomDatabaseName(),
				},
			},
			Response: &Response{
				StatusCode: 201,
				Content:    map[string]any{},
				Captures:   []string{"databaseName", "branchName"},
			},
		})

		// Write data to database
		steps = append(steps, TestStep{
			Request: &Request{
				Name:      "Write data to database for export",
				Model:     "Query",
				Operation: "createQuery",
				Body: map[string]any{
					"queries": []any{
						map[string]any{
							"id":         fmt.Sprintf("query-%d", time.Now().UTC().Nanosecond()),
							"statement":  "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO test_table (value) VALUES ('test export data');",
							"parameters": map[string]any{},
						},
					},
				},
				Parameters: []string{"databaseName", "branchName"},
			},
			Response: &Response{
				StatusCode: 200,
				Content:    map[string]any{},
			},
		})
	}

	// Step 5: when creating an ImportChunk, we need an import first
	if resourceType == "ImportChunk" {
		steps = append(steps, generateImportPrerequisites()...)
	}

	// Step 6: when creating a DatabaseExportEnd, we need to create an export first
	if resourceType == "DatabaseExportEnd" {
		steps = append(steps, TestStep{
			Request: &Request{
				Name:       "Create DatabaseExport",
				Model:      "DatabaseExport",
				Operation:  "createDatabaseExport",
				Body:       nil,
				Parameters: []string{"databaseName", "branchName"},
			},
			Response: &Response{
				StatusCode: 201,
				Content:    nil,
				Captures:   []string{"databaseName", "databaseBranchName AS branchName", "id AS exportId", "rangeCount"},
			},
		})
	}

	// Final step: Create the actual resource
	steps = append(steps, TestStep{
		Request: &Request{
			Name:       fmt.Sprintf("Create %s", resourceType),
			Model:      resourceType,
			Operation:  operationID,
			Body:       generateRequestBody(resourceType, "create"),
			Parameters: buildParametersFromPath(details),
		},
		Response: &Response{
			StatusCode: 201,
			Content:    map[string]any{},
			Captures:   generateCapturesForCreate(resourceType),
		},
	})

	return TestCase{
		OperationID: operationID,
		Name:        fmt.Sprintf("Test %s - Create resource", operationID),
		Description: fmt.Sprintf("Creates a new %s and verifies the response", resourceType),
		Steps:       steps,
	}
}

// buildParametersFromPath builds the parameters array for a request based on the path parameters
func buildParametersFromPath(details map[string]any) []string {
	// Extract parameters from the path itself
	pathParams := extractPathParameters(details)

	// Return parameters in camelCase (as they are in the OpenAPI spec)
	return pathParams
}

// generateDatabaseBackupPrerequisites generates the steps needed before creating a DatabaseBackup
// This includes: creating database, creating branch, writing data, and waiting for checkpoint
func generateDatabaseBackupPrerequisites() []TestStep {
	steps := []TestStep{}

	// Step 1: Create database
	steps = append(steps, TestStep{
		Request: &Request{
			Name:      "Create test Database",
			Model:     "Database",
			Operation: "createDatabase",
			Body: map[string]any{
				"name": generateRandomDatabaseName(),
			},
		},
		Response: &Response{
			StatusCode: 201,
			Content:    map[string]any{},
			Captures:   []string{"databaseName", "branchName"},
		},
	})

	// Step 3: Write data to trigger snapshot
	steps = append(steps, TestStep{
		Request: &Request{
			Name:      "Write data to database to trigger snapshot",
			Model:     "Query",
			Operation: "createQuery",
			Body: map[string]any{
				"queries": []any{
					map[string]any{
						"id":         fmt.Sprintf("query-%d", time.Now().UTC().Nanosecond()),
						"statement":  "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO test_table (value) VALUES ('test');",
						"parameters": map[string]any{},
					},
				},
			},
			Parameters: []string{"databaseName", "branchName"},
		},
		Response: &Response{
			StatusCode: 200,
			Content:    map[string]any{},
		},
	})

	// Step 4: Wait for checkpoint
	steps = append(steps, TestStep{
		Wait: &WaitStep{
			Name:     "Wait for database checkpoint",
			Duration: 1000, // DatabaseCheckpointThreshold = 1 second
			Reason:   "Database snapshots are only created after a checkpoint, which occurs every 1 second after writes",
		},
	})

	// Step 5: List snapshots to capture restore point timestamp for backup
	steps = append(steps, TestStep{
		Request: &Request{
			Name:       "List DatabaseSnapshots to capture restore point",
			Model:      "DatabaseSnapshot",
			Operation:  "listDatabaseSnapshots",
			Body:       map[string]any{},
			Parameters: []string{"databaseName", "branchName"},
		},
		Response: &Response{
			StatusCode: 200,
			Content: map[string]any{
				"data": []any{},
			},
			Captures: []string{
				"data[0].timestamp AS timestamp",
			},
		},
	})

	return steps
}

// generateDatabaseBranchPrerequisites generates the steps needed before creating a DatabaseBranch
// This includes: creating database, writing data, and waiting for checkpoint
func generateDatabaseBranchPrerequisites() []TestStep {
	steps := []TestStep{}

	// Create parent database
	steps = append(steps, TestStep{
		Request: &Request{
			Name:      "Create test Database",
			Model:     "Database",
			Operation: "createDatabase",
			Body: map[string]any{
				"name": generateRandomDatabaseName(),
			},
		},
		Response: &Response{
			StatusCode: 201,
			Content:    map[string]any{},
			Captures:   []string{"databaseName", "branchName"},
		},
	})

	// Write data to database before creating branch
	steps = append(steps, TestStep{
		Request: &Request{
			Name:      "Write data to database before creating branch",
			Model:     "Query",
			Operation: "createQuery",
			Body: map[string]any{
				"queries": []any{
					map[string]any{
						"id":         fmt.Sprintf("query-%d", time.Now().UTC().Nanosecond()),
						"statement":  "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO test_table (value) VALUES ('test');",
						"parameters": map[string]any{},
					},
				},
			},
			Parameters: []string{"databaseName", "branchName"},
		},
		Response: &Response{
			StatusCode: 200,
			Content:    map[string]any{},
		},
	})

	// Wait for checkpoint
	steps = append(steps, TestStep{
		Wait: &WaitStep{
			Name:     "Wait for database checkpoint",
			Duration: 1000,
			Reason:   "Database branches are created from the latest checkpoint, which occurs every 1 second after writes",
		},
	})

	return steps
}

// generateDatabaseRestorePrerequisites generates the steps needed before restoring a Database
// This includes: creating database, creating branch, writing data, and waiting for checkpoint
func generateDatabaseRestorePrerequisites() []TestStep {
	steps := []TestStep{}

	// Step 1: Create database
	steps = append(steps, TestStep{
		Request: &Request{
			Name:      "Create test Database",
			Model:     "Database",
			Operation: "createDatabase",
			Body: map[string]any{
				"name": generateRandomDatabaseName(),
			},
		},
		Response: &Response{
			StatusCode: 201,
			Content:    map[string]any{},
			Captures:   []string{"databaseName", "branchName"},
		},
	})

	// Step 2: Write data to trigger snapshot
	steps = append(steps, TestStep{
		Request: &Request{
			Name:      "Write data to database to trigger snapshot",
			Model:     "Query",
			Operation: "createQuery",
			Body: map[string]any{
				"queries": []any{
					map[string]any{
						"id":         fmt.Sprintf("query-%d", time.Now().UTC().Nanosecond()),
						"statement":  "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO test_table (value) VALUES ('test');",
						"parameters": map[string]any{},
					},
				},
			},
			Parameters: []string{"databaseName", "branchName"},
		},
		Response: &Response{
			StatusCode: 200,
			Content:    map[string]any{},
		},
	})

	// Step 3: Wait for checkpoint/snapshot
	steps = append(steps, TestStep{
		Wait: &WaitStep{
			Name:     "Wait for database checkpoint and snapshot",
			Duration: 1000, // DatabaseCheckpointThreshold = 1 second
			Reason:   "Database snapshots are only created after a checkpoint, which occurs every 1 second after writes",
		},
	})

	// Step 4: List snapshots to capture restore point timestamp
	steps = append(steps, TestStep{
		Request: &Request{
			Name:       "List DatabaseSnapshots to capture restore point",
			Model:      "DatabaseSnapshot",
			Operation:  "listDatabaseSnapshots",
			Body:       map[string]any{},
			Parameters: []string{"databaseName", "branchName"},
		},
		Response: &Response{
			StatusCode: 200,
			Content: map[string]any{
				"data": []any{},
			},
			Captures: []string{
				"data[0].timestamp AS timestamp",
			},
		},
	})

	// Create a target database for restore
	steps = append(steps, TestStep{
		Request: &Request{
			Name:      "Create target Database for restore",
			Model:     "Database",
			Operation: "createDatabase",
			Body: map[string]any{
				"name": generateRandomDatabaseName(),
			},
		},
		Response: &Response{
			StatusCode: 201,
			Content:    map[string]any{},
			Captures:   []string{"databaseName AS targetDatabaseName", "branchName AS targetBranchName"},
		},
	})

	return steps
}

// generateDatabaseSnapshotPrerequisites generates the steps needed before accessing a DatabaseSnapshot
// This includes: creating database, creating branch, writing data, and waiting for checkpoint
func generateDatabaseSnapshotPrerequisites() []TestStep {
	steps := []TestStep{}

	// Step 1: Create database
	steps = append(steps, TestStep{
		Request: &Request{
			Name:      "Create test Database",
			Model:     "Database",
			Operation: "createDatabase",
			Body: map[string]any{
				"name": generateRandomDatabaseName(),
			},
		},
		Response: &Response{
			StatusCode: 201,
			Content:    map[string]any{},
			Captures:   []string{"databaseName", "branchName"},
		},
	})

	// Step 2: Write data to trigger snapshot
	steps = append(steps, TestStep{
		Request: &Request{
			Name:      "Write data to database to trigger snapshot",
			Model:     "Query",
			Operation: "createQuery",
			Body: map[string]any{
				"queries": []any{
					map[string]any{
						"id":         fmt.Sprintf("query-%d", time.Now().UTC().Nanosecond()),
						"statement":  "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO test_table (value) VALUES ('test');",
						"parameters": map[string]any{},
					},
				},
			},
			Parameters: []string{"databaseName", "branchName"},
		},
		Response: &Response{
			StatusCode: 200,
			Content:    map[string]any{},
		},
	})

	// Step 3: Wait for checkpoint/snapshot
	steps = append(steps, TestStep{
		Wait: &WaitStep{
			Name:     "Wait for database checkpoint and snapshot",
			Duration: 1000, // DatabaseCheckpointThreshold = 1 second
			Reason:   "Database snapshots are only created after a checkpoint, which occurs every 1 second after writes",
		},
	})

	return steps
}

func generateQueryLogPrerequisites() []TestStep {
	steps := []TestStep{}

	// Step 1: Create database
	steps = append(steps, TestStep{
		Request: &Request{
			Name:      "Create test Database",
			Model:     "Database",
			Operation: "createDatabase",
			Body: map[string]any{
				"name": generateRandomDatabaseName(),
			},
		},
		Response: &Response{
			StatusCode: 201,
			Content:    map[string]any{},
			Captures:   []string{"databaseName", "branchName"},
		},
	})

	// Step 2: Write data to trigger query log
	steps = append(steps, TestStep{
		Request: &Request{
			Name:      "Write data to database to trigger query log",
			Model:     "Query",
			Operation: "createQuery",
			Body: map[string]any{
				"queries": []any{
					map[string]any{
						"id":         fmt.Sprintf("query-%d", time.Now().UTC().Nanosecond()),
						"statement":  "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO test_table (value) VALUES ('test');",
						"parameters": map[string]any{},
					},
				},
			},
			Parameters: []string{"databaseName", "branchName"},
		},
		Response: &Response{
			StatusCode: 200,
			Content:    map[string]any{},
		},
	})

	return steps
}

func generateErrorLogPrerequisites() []TestStep {
	steps := []TestStep{}

	// Step 1: Create database
	steps = append(steps, TestStep{
		Request: &Request{
			Name:      "Create test Database",
			Model:     "Database",
			Operation: "createDatabase",
			Body: map[string]any{
				"name": generateRandomDatabaseName(),
			},
		},
		Response: &Response{
			StatusCode: 201,
			Content:    map[string]any{},
			Captures:   []string{"databaseName", "branchName"},
		},
	})

	// Step 2: Execute query that will cause an error to trigger error log
	steps = append(steps, TestStep{
		Request: &Request{
			Name:      "Execute query with error to trigger error log",
			Model:     "Query",
			Operation: "createQuery",
			Body: map[string]any{
				"queries": []any{
					map[string]any{
						"id":         fmt.Sprintf("query-%d", time.Now().UTC().Nanosecond()),
						"statement":  "SELECT * FROM nonexistent_table;",
						"parameters": map[string]any{},
					},
				},
			},
			Parameters: []string{"databaseName", "branchName"},
		},
		Response: &Response{
			StatusCode: 200,
			Content:    map[string]any{},
		},
	})

	return steps
}

// generateDatabaseExportPrerequisites generates the steps needed before accessing a DatabaseExport
// This includes: creating database, creating branch, writing data, and creating export
func generateDatabaseExportPrerequisites() []TestStep {
	steps := []TestStep{}

	// Step 1: Create database
	steps = append(steps, TestStep{
		Request: &Request{
			Name:      "Create test Database",
			Model:     "Database",
			Operation: "createDatabase",
			Body: map[string]any{
				"name": generateRandomDatabaseName(),
			},
		},
		Response: &Response{
			StatusCode: 201,
			Content:    map[string]any{},
			Captures:   []string{"databaseName", "branchName"},
		},
	})

	// Step 2: Write data to database
	steps = append(steps, TestStep{
		Request: &Request{
			Name:      "Write data to database for export",
			Model:     "Query",
			Operation: "createQuery",
			Body: map[string]any{
				"queries": []any{
					map[string]any{
						"id":         fmt.Sprintf("query-%d", time.Now().UTC().Nanosecond()),
						"statement":  "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO test_table (value) VALUES ('test export data');",
						"parameters": map[string]any{},
					},
				},
			},
			Parameters: []string{"databaseName", "branchName"},
		},
		Response: &Response{
			StatusCode: 200,
			Content:    map[string]any{},
		},
	})

	// Step 3: Create export
	// Note: This creates an export session that returns export metadata.
	// The session must be explicitly ended with createDatabaseExportEnd after retrieving parts.
	steps = append(steps, TestStep{
		Request: &Request{
			Name:       "Create DatabaseExport",
			Model:      "DatabaseExport",
			Operation:  "createDatabaseExport",
			Body:       nil,
			Parameters: []string{"databaseName", "branchName"},
		},
		Response: &Response{
			StatusCode: 201,
			Content:    nil,
			Captures:   []string{"databaseName", "databaseBranchName AS branchName", "id AS exportId", "rangeCount", "ranges[0] AS rangeNumber"},
		},
	})

	return steps
}

// generateImportPrerequisites generates the steps needed before creating ImportChunks
// This includes: creating an import
func generateImportPrerequisites() []TestStep {
	steps := []TestStep{}

	// Step 1: Create import
	steps = append(steps, TestStep{
		Request: &Request{
			Name:      "Create Import",
			Model:     "Import",
			Operation: "createImport",
			Body: map[string]any{
				"databaseName": generateRandomDatabaseName(),
				"chunkCount":   1,
			},
		},
		Response: &Response{
			StatusCode: 201,
			Content:    map[string]any{},
			Captures:   []string{"importId", "chunkCount"},
		},
	})

	return steps
}

// generateGetTestCase generates a test for get operations
func generateGetTestCase(operationID, resourceType string, details map[string]any) TestCase {
	createOp := getCreateOperationID(resourceType)

	steps := []TestStep{}

	if resourceType == "DatabaseBranch" {
		// Special handling for DatabaseBranch - needs database, write, and wait
		steps = append(steps, generateDatabaseBranchPrerequisites()...)
	} else if resourceType == "DatabaseBranchSettings" {
		// Special handling for DatabaseBranchSettings - needs database (main branch created automatically)
		steps = append(steps, TestStep{
			Request: &Request{
				Name:      "Create test Database",
				Model:     "Database",
				Operation: "createDatabase",
				Body: map[string]any{
					"name": generateRandomDatabaseName(),
				},
				RequestModel: "DatabaseStoreRequest",
			},
			Response: &Response{
				StatusCode: 201,
				Content:    map[string]any{},
				Captures: []string{
					"databaseName",
					"branchName",
				},
			},
		})
		// Special handling for DatabaseBackup - needs database, branch, write, and wait
	} else if resourceType == "DatabaseBackup" {
		steps = append(steps, generateDatabaseBackupPrerequisites()...)

		steps = append(steps, TestStep{
			Request: &Request{
				Name:       "Create test DatabaseBackup",
				Model:      "DatabaseBackup",
				Operation:  "createDatabaseBackup",
				Body:       map[string]any{},
				Parameters: []string{"databaseName", "branchName"},
			},
			Response: &Response{
				StatusCode: 201,
				Content:    map[string]any{},
				Captures:   generateCapturesForCreate(resourceType),
			},
		})
		// DatabaseBackup prerequisites already include the create step, so skip the normal create
	} else if resourceType == "DatabaseSnapshot" {
		// Special handling for DatabaseSnapshot - needs database, branch, write, and wait (no create operation)
		steps = append(steps, generateDatabaseSnapshotPrerequisites()...)

		// For get operation, we need to list snapshots first to capture a timestamp
		listOp := getListOperationID(resourceType)
		if operationExists(listOp) {
			steps = append(steps, TestStep{
				Request: &Request{
					Name:       fmt.Sprintf("List %s to capture timestamp", getPluralResourceName(resourceType)),
					Model:      resourceType,
					Operation:  listOp,
					Body:       map[string]any{},
					Parameters: []string{"databaseName", "branchName"},
				},
				Response: &Response{
					StatusCode: 200,
					Content: map[string]any{
						"data": []any{},
					},
					Captures: []string{"data[0].timestamp AS timestamp"},
				},
			})
		}
	} else if resourceType == "DatabaseExportPart" {
		// Special handling for DatabaseExportPart - needs database, branch, and export
		steps = append(steps, generateDatabaseExportPrerequisites()...)
	} else if createOp != "" && operationExists(createOp) {
		// Step 1: Create the resource (only if create operation exists)
		steps = append(steps, TestStep{
			Request: &Request{
				Name:       fmt.Sprintf("Create test %s", resourceType),
				Model:      resourceType,
				Operation:  createOp,
				Body:       generateRequestBody(resourceType, "create"),
				Parameters: getOperationParameters(createOp),
			},
			Response: &Response{
				StatusCode: 201,
				Content:    map[string]any{},
				Captures:   generateCapturesForCreate(resourceType),
			},
		})
	}

	// Step 2: Get the resource
	isBinaryFile := resourceType == "DatabaseExportPart" // Export parts return binary file data
	steps = append(steps, TestStep{
		Request: &Request{
			Name:       fmt.Sprintf("Get the %s resource", resourceType),
			Model:      resourceType,
			Operation:  operationID,
			Body:       map[string]any{},
			Parameters: extractPathParameters(details),
		},
		Response: &Response{
			StatusCode:   200,
			Content:      map[string]any{},
			IsBinaryFile: isBinaryFile,
		},
	})

	// Step 3: Cleanup - end export session if this is an export part test
	if resourceType == "DatabaseExportPart" {
		steps = append(steps, TestStep{
			Request: &Request{
				Name:       "End DatabaseExport session",
				Model:      "DatabaseExportEnd",
				Operation:  "createDatabaseExportEnd",
				Parameters: []string{"databaseName", "branchName", "exportId"},
			},
			Response: &Response{
				StatusCode: 204,
			},
		})
	}

	return TestCase{
		OperationID: operationID,
		Name:        fmt.Sprintf("Test %s - Create and retrieve resource", operationID),
		Description: fmt.Sprintf("Creates a %s and retrieves it by ID", resourceType),
		Steps:       steps,
	}
}

// generateUpdateTestCase generates a test for update operations
func generateUpdateTestCase(operationID, resourceType string, details map[string]any) TestCase {
	createOp := getCreateOperationID(resourceType)

	steps := []TestStep{}

	// Special handling for DatabaseBranchSettings - needs database (main branch created automatically)
	if resourceType == "DatabaseBranchSettings" {
		steps = append(steps, TestStep{
			Request: &Request{
				Name:      "Create test Database",
				Model:     "Database",
				Operation: "createDatabase",
				Body: map[string]any{
					"name": generateRandomDatabaseName(),
				},
				RequestModel: "DatabaseStoreRequest",
			},
			Response: &Response{
				StatusCode: 201,
				Content:    map[string]any{},
				Captures: []string{
					"databaseName",
					"branchName",
				},
			},
		})
		// Special handling for DatabaseBackup - needs database, branch, write, and wait
	} else if resourceType == "DatabaseBackup" {
		steps = append(steps, generateDatabaseBackupPrerequisites()...)
		// DatabaseBackup prerequisites already include the create step, so skip the normal create
	} else if createOp != "" && operationExists(createOp) {
		// Step 1: Create the resource (only if create operation exists)
		steps = append(steps, TestStep{
			Request: &Request{
				Name:       fmt.Sprintf("Create test %s", resourceType),
				Model:      resourceType,
				Operation:  createOp,
				Body:       generateRequestBody(resourceType, "create"),
				Parameters: getOperationParameters(createOp),
			},
			Response: &Response{
				StatusCode: 201,
				Content:    map[string]any{},
				Captures:   generateCapturesForCreate(resourceType),
			},
		})
	}

	// Step 2: Update the resource
	steps = append(steps, TestStep{
		Request: &Request{
			Name:       fmt.Sprintf("Update %s", resourceType),
			Model:      resourceType,
			Operation:  operationID,
			Body:       generateRequestBody(resourceType, "update"),
			Parameters: extractPathParameters(details),
		},
		Response: &Response{
			StatusCode: 200,
			Content:    map[string]any{},
		},
	})

	return TestCase{
		OperationID: operationID,
		Name:        fmt.Sprintf("Test %s - Create, update and verify", operationID),
		Description: fmt.Sprintf("Creates a %s, updates it, and verifies the changes", resourceType),
		Steps:       steps,
	}
}

// generateDeleteTestCase generates a test for delete operations
func generateDeleteTestCase(operationID, resourceType string, details map[string]any) TestCase {
	createOp := getCreateOperationID(resourceType)

	steps := []TestStep{}

	// Special handling for DatabaseBackup - needs database, branch, write, wait, and list snapshots
	if resourceType == "DatabaseBackup" {
		steps = append(steps, generateDatabaseBackupPrerequisites()...)

		// Create the backup with proper parameters
		steps = append(steps, TestStep{
			Request: &Request{
				Name:       "Create DatabaseBackup",
				Model:      "DatabaseBackup",
				Operation:  createOp,
				Body:       generateRequestBody(resourceType, "create"),
				Parameters: []string{"databaseName", "branchName"},
			},
			Response: &Response{
				StatusCode: 201,
				Content:    map[string]any{},
				Captures:   []string{"restorePoint.timestamp AS timestamp"},
			},
		})

		// Delete the backup
		steps = append(steps, TestStep{
			Request: &Request{
				Name:       "Delete DatabaseBackup",
				Model:      "DatabaseBackup",
				Operation:  operationID,
				Body:       map[string]any{},
				Parameters: extractPathParameters(details),
			},
			Response: &Response{
				StatusCode: 204,
				Content:    map[string]any{},
			},
		})

		// Verify deletion by getting the resource and expecting 404
		getOp := getGetOperationID(resourceType)
		if operationExists(getOp) {
			steps = append(steps, TestStep{
				Request: &Request{
					Name:       "Verify DatabaseBackup is deleted",
					Model:      "DatabaseBackup",
					Operation:  getOp,
					Body:       map[string]any{},
					Parameters: extractPathParameters(details),
				},
				Response: &Response{
					StatusCode: 404,
				},
			})
		}

		return TestCase{
			OperationID: operationID,
			Name:        fmt.Sprintf("Test %s - Create, delete and verify", operationID),
			Description: fmt.Sprintf("Creates a %s, deletes it, and verifies deletion", resourceType),
			Steps:       steps,
		}
	}

	// Special handling for DatabaseBranch - needs parent database first
	if resourceType == "DatabaseBranch" {
		steps = append(steps, generateDatabaseBranchPrerequisites()...)
	}

	// Step 1: Create the resource (only if create operation exists)
	if createOp != "" && operationExists(createOp) {
		steps = append(steps, TestStep{
			Request: &Request{
				Name:       fmt.Sprintf("Create test %s", resourceType),
				Model:      resourceType,
				Operation:  createOp,
				Body:       generateRequestBody(resourceType, "create"),
				Parameters: getOperationParameters(createOp),
			},
			Response: &Response{
				StatusCode: 201,
				Content:    map[string]any{},
				Captures:   generateCapturesForCreate(resourceType),
			},
		})
	}

	// Step 2: Delete the resource
	steps = append(steps, TestStep{
		Request: &Request{
			Name:       fmt.Sprintf("Delete %s", resourceType),
			Model:      resourceType,
			Operation:  operationID,
			Body:       map[string]any{},
			Parameters: extractPathParameters(details),
		},
		Response: &Response{
			StatusCode: 204,
			Content:    map[string]any{},
		},
	})

	// Step 3: Verify deletion by getting the resource and expecting 404
	getOp := getGetOperationID(resourceType)
	if operationExists(getOp) {
		steps = append(steps, TestStep{
			Request: &Request{
				Name:       fmt.Sprintf("Verify %s is deleted", resourceType),
				Model:      resourceType,
				Operation:  getOp,
				Body:       map[string]any{},
				Parameters: extractPathParameters(details),
			},
			Response: &Response{
				StatusCode: 404,
			},
		})
	}

	return TestCase{
		OperationID: operationID,
		Name:        fmt.Sprintf("Test %s - Create, delete and verify", operationID),
		Description: fmt.Sprintf("Creates a %s, deletes it, and verifies deletion", resourceType),
		Steps:       steps,
	}
}

// generateGenericTestCase generates a generic test case for other operations
func generateGenericTestCase(operationID, resourceType string) TestCase {
	return TestCase{
		OperationID: operationID,
		Name:        fmt.Sprintf("Test %s", operationID),
		Description: fmt.Sprintf("Test for %s operation", operationID),
		Steps: []TestStep{
			{
				Request: &Request{
					Name:      fmt.Sprintf("Execute %s", operationID),
					Model:     resourceType,
					Operation: operationID,
					Body:      generateRequestBody(resourceType, "other"),
				},
				Response: &Response{
					StatusCode: 200,
					Content:    map[string]any{},
				},
			},
		},
	}
}

// getCreateOperationID returns the create operation ID for a given resource type
func getCreateOperationID(resourceType string) string {
	return "create" + resourceType
}

// getGetOperationID returns the get operation ID for a given resource type
func getGetOperationID(resourceType string) string {
	return "get" + resourceType
}

// generateRandomDatabaseName creates a unique random database name
func generateRandomDatabaseName() string {
	// Generate 8 random bytes
	bytes := make([]byte, 8)

	if _, err := rand.Read(bytes); err != nil {
		// Fallback to a simple timestamp-based name if random fails
		return fmt.Sprintf("test_db_%d", len(bytes))
	}

	// Convert to hex and prefix with "test_db_"
	return fmt.Sprintf("test_db_%s", hex.EncodeToString(bytes))
}

// generateCapturesForCreate returns the list of fields to capture from a create response.
// Supports nested object extraction using dot notation (e.g., "restorePoint.timestamp").
// The field names should match the JSON property names (camelCase) from the API response.
func generateCapturesForCreate(resourceType string) []string {
	lowerResource := strings.ToLower(resourceType)

	// Define captures based on resource type (using camelCase for JSON field names)
	// For nested objects, use dot notation: "parentField.childField"
	switch lowerResource {
	case "accesskey":
		return []string{"accessKeyId", "accessKeySecret"}
	case "database":
		return []string{"databaseName", "branchName"}
	case "databasebranch":
		return []string{"databaseName", "name AS branchName"}
	case "databasebackup":
		return []string{
			"restorePoint.timestamp AS timestamp",
		}
	case "token":
		return []string{"tokenId", "token"}
	case "user":
		return []string{"username"}
	case "databasesnapshot":
		return []string{"timestamp"}
	case "query":
		return []string{"data[0].id"}
	case "databaseexport":
		// Export returns: {id, rangeCount, startedAt}
		// This is a streaming response - connection stays open while chunks are fetched
		return []string{"databaseName", "databaseBranchName AS branchName", "id AS exportId", "rangeCount"}
	case "import":
		return []string{"importId", "chunkCount"}
	default:
		return []string{}
	}
}

// generateRequestBody generates a request body for create and update operations
func generateRequestBody(resourceType, operation string) map[string]any {
	lowerResource := strings.ToLower(resourceType)

	switch lowerResource {
	case "accesskey":
		if operation == "create" {
			return map[string]any{
				"description": "test-access",
				"statements": []map[string]any{
					{
						"effect":   "allow",
						"resource": "*",
						"actions":  []auth.Privilege{"*"},
					},
				},
			}
		}
		if operation == "update" {
			return map[string]any{
				"description": "Updated test access key",
				"statements": []map[string]any{
					{
						"effect":   "allow",
						"resource": "*",
						"actions":  []auth.Privilege{"*"},
					},
				},
			}
		}

	case "database":
		if operation == "create" {
			return map[string]any{
				"name": generateRandomDatabaseName(),
			}
		}

	case "databasebranch":
		if operation == "create" {
			return map[string]any{
				"name": "test-branch",
			}
		}

	case "databasebackup":
		if operation == "create" {
			return map[string]any{}
		}

	case "databasebranchsettings":
		if operation == "update" {
			return map[string]any{
				"backupsEnabled":                  true,
				"backupInterval":                  "24h",
				"backupsRetentionDays":            7,
				"incrementalBackupsEnabled":       true,
				"incrementalBackupsRetentionDays": 30,
				"queryLogsEnabled":                true,
				"queryLogsRetentionDays":          7,
				"errorLogsEnabled":                true,
				"errorLogsRetentionDays":          7,
				"defaultPragmas": map[string]any{
					"foreignKeys": "ON",
				},
			}
		}

	case "token":
		if operation == "create" {
			return map[string]any{
				"description": "Test token for SDK testing",
				"statements": []map[string]any{
					{
						"effect":   "allow",
						"resource": "*",
						"actions":  []auth.Privilege{"*"},
					},
				},
			}
		}
		if operation == "update" {
			return map[string]any{
				"description": "Updated test token",
				"statements": []map[string]any{
					{
						"effect":   "allow",
						"resource": "*",
						"actions":  []auth.Privilege{"*"},
					},
				},
			}
		}

	case "user":
		if operation == "create" {
			return map[string]any{
				"username":    fmt.Sprintf("test_user_sdk_%d", time.Now().UTC().Nanosecond()),
				"password":    "test_password_123",
				"description": "Test user for SDK testing",
				"statements": []map[string]any{
					{
						"effect":   "allow",
						"resource": "*",
						"actions":  []auth.Privilege{"*"},
					},
				},
			}
		}
		if operation == "update" {
			return map[string]any{
				"username":    "{{username}}",
				"description": "Updated test user",
				"statements": []map[string]any{
					{
						"effect":   "allow",
						"resource": "*",
						"actions":  []auth.Privilege{"*"},
					},
				},
			}
		}

	case "query":
		return map[string]any{
			"queries": []any{
				map[string]any{
					"id":         fmt.Sprintf("query-%d", time.Now().UTC().Nanosecond()),
					"statement":  "SELECT 1",
					"parameters": map[string]any{},
				},
			},
		}

	case "databaseexport":
		return map[string]any{}

	case "import":
		return map[string]any{
			"databaseName": generateRandomDatabaseName(),
			"chunkCount":   1,
		}

	case "importchunk":
		return map[string]any{
			"chunkIndex": 0,
			"chunkData":  "U1FMaXRlIGZvcm1hdCAzABAAAQEAQCAgAAAAAgAAAAAAAAAAAAAAAAEAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		}

	case "databaserestore":
		return map[string]any{
			"targetDatabase":       "{{targetDatabaseName}}",
			"targetDatabaseBranch": "{{targetBranchName}}",
			"timestamp":            "{{timestamp}}",
		}

	case "key":
		encryptionKey := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		hash := hmac.New(sha256.New, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
		hash.Write([]byte(encryptionKey))
		hmacHexSignature := fmt.Sprintf("%x", hash.Sum(nil))

		return map[string]any{
			"encryptionKey": encryptionKey,
			"signature":     hmacHexSignature,
		}

	case "keyactivate":
		return map[string]any{
			"encryptionKey": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		}
	}

	return map[string]any{}
}

// getRequestModelForOperationID searches the loaded OpenAPI paths for the operation
// and returns the simple schema name referenced by the operation's requestBody
// (e.g. "#/components/schemas/TokenStoreRequest" -> "TokenStoreRequest").
func getRequestModelForOperationID(operationID string) string {
	for _, methods := range openAPIPaths {
		if methodMap, ok := methods.(map[string]any); ok {
			for _, detail := range methodMap {
				if details, ok := detail.(map[string]any); ok {
					if opid, ok := details["operationId"].(string); ok && opid == operationID {
						if rb, ok := details["requestBody"].(map[string]any); ok {
							if content, ok := rb["content"].(map[string]any); ok {
								if appJson, ok := content["application/json"].(map[string]any); ok {
									if schema, ok := appJson["schema"].(map[string]any); ok {
										if ref, ok := schema["$ref"].(string); ok {
											parts := strings.Split(ref, "/")
											return parts[len(parts)-1]
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}

	return ""
}
