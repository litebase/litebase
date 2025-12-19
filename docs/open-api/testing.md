# OpenAPI Generator Testing Guide

This guide explains how to ensure the OpenAPI generator continues to work correctly when adding or modifying controllers.

## Overview

The OpenAPI generator in `pkg/openapi/generator.go` automatically extracts OpenAPI specifications from Go controller code using route reflection. To ensure it continues working correctly as the codebase evolves, we have comprehensive tests in `pkg/openapi/generator_test.go`.

## Testing Strategy

### 1. Route Analysis Tests

These tests verify that the generator can:

- Detect all controller methods using route reflection
- Extract type information from request/response structs  
- Generate proper OpenAPI operations with HTTP methods and paths
- Handle dynamic pluralization and naming conventions

### 2. Schema Generation Tests

These tests verify that the generator properly:

- Extracts response schemas from controller return types
- Generates request body schemas from controller parameters
- Maps validation tags to OpenAPI constraints
- Handles complex nested types and arrays

### 3. Text Processing Tests

Tests for the dynamic text processing functions:

- `pluralize()` and `singularize()` functions work correctly
- Compound word detection via `hasCompoundStructure()`
- Display name generation from resource names
- Operation summary and ID generation

### 4. Cross-Controller Consistency Tests

This test suite can be easily extended when new controllers are added:

```go
// Example test case structure
testCases := []struct {
    name                    string
    controllerFile          string
    controllerName          string
    expectedMethodCount     int
    expectTypes            bool
    expectedTypePatterns   []string
}{
    {
        name:                "UserController", 
        controllerFile:      "../http/user_controller.go",
        controllerName:      "UserController",
        expectedMethodCount: 5,
        expectTypes:        true,
        expectedTypePatterns: []string{"UserControllerStoreRequest", "UserControllerUpdateRequest"},
    },
    // Add new controllers here
}
```

### 5. Integration Tests

Full end-to-end tests that:

- Generate complete OpenAPI specifications
- Validate JSON schema compliance
- Test security scheme generation
- Verify tag and operation metadata

## Adding New Controllers

When adding a new controller, follow this checklist:

### 1. Controller Naming Convention

Ensure your controller follows the consistent naming pattern:

- Controller file: `{resource}_controller.go` (e.g., `user_controller.go`)
- Controller methods: `{Resource}Controller{Action}` (e.g., `UserControllerIndex`, `UserControllerStore`)
- Actions: `Index`, `Show`, `Store`, `Update`, `Destroy`

### 2. Request Type Definitions

Define request types in the same file as your controller:

```go
type YourControllerStoreRequest struct {
    Field1 string `json:"field1" validate:"required"`
    Field2 int    `json:"field2" validate:"min=1"`
}

type YourControllerUpdateRequest struct {
    Field1 string `json:"field1"`
    Field2 int    `json:"field2" validate:"min=1"`
}
```

### 3. Response Patterns

Use consistent response patterns:

```go
// For collections - return array in data field
return Response{
    StatusCode: 200,
    Body: map[string]any{
        "data": responseArray,
    },
}

// For individual resources - use SuccessResponse helper
return SuccessResponse(userResponse)

// Or return individual resource directly  
return Response{
    StatusCode: 200,
    Body: userResponse,
}
```

### 4. Update Tests

Add your controller to the test cases in the generator tests:

```go
{
    name:                "YourController",
    controllerFile:      "../http/your_controller.go", 
    controllerName:      "YourController",
    expectedMethodCount: 5, // Adjust based on methods you implement
    expectTypes:        true,
    expectedTypePatterns: []string{"YourControllerStoreRequest", "YourControllerUpdateRequest"},
},
```

## Running Tests

Run the generator tests to ensure everything works:

```bash
go test ./pkg/openapi/... -v
```

For continuous integration, run:

```bash
go test ./pkg/openapi/... -race -cover
```

## Testing the Generator

### Manual Testing

Generate the OpenAPI specification to verify it works:

```bash
go run cmd/generate_open_api_spec/main.go
```

This will create `api/generated_open_api.json` with the complete specification.

### Validation

Verify the generated specification:

```bash
# View the generated spec
cat api/generated_open_api.json | jq .

# Check specific controllers
grep -o '"tags": \["[^"]*"\]' api/generated_open_api.json | sort | uniq

# Check operation summaries
grep -o '"summary": "[^"]*"' api/generated_open_api.json
```

## Debugging

If the generator isn't working correctly:

1. **Check the logs** - the generator provides detailed logging during execution

2. **Verify controller naming** - methods must follow the ResourceControllerAction pattern

3. **Check struct definitions** - they must be in the same file as the controller

4. **Test individual functions** - use the Go test framework to test specific functions

## Common Issues

### Types Not Detected

- **Cause:** Struct not defined in same file as controller
- **Solution:** Move struct definition to controller file

### Methods Not Detected  

- **Cause:** Controller method doesn't follow naming convention
- **Solution:** Rename method to follow `{Resource}Controller{Action}` pattern

### Response Schema Empty

- **Cause:** Response body doesn't match expected patterns
- **Solution:** Use consistent response patterns (see Response Patterns section)

### Request Body Missing

- **Cause:** No matching request type found for Store/Update methods
- **Solution:** Define `{Resource}ControllerStoreRequest` and `{Resource}ControllerUpdateRequest` types

### Incorrect Pluralization

- **Cause:** Dynamic pluralization not working for compound words
- **Solution:** The system now handles compound words automatically via camelCase/PascalCase analysis

## Performance Considerations

The generator is designed to be fast, but for large codebases:

- Consider running tests in parallel: `go test -parallel 4 ./pkg/openapi/...`
- Use benchmarks to track performance: `go test -bench=. ./pkg/openapi/...`
- The current implementation processes 40+ methods in milliseconds

## Extending the Generator

If you need to extend the generator for new patterns:

1. **Add detection logic** in `generator.go`
2. **Add corresponding tests** in `generator_test.go`  
3. **Update this documentation** with new patterns
4. **Run full test suite** to ensure no regressions

The test suite is designed to catch regressions early and ensure the generator remains reliable as the codebase grows.

## Import/Export Testing Scheme

The test case generator includes special handling for database import and export operations, which require multi-step workflows with proper prerequisites.

### Database Export Flow

Exports use a **session-based pattern** with automatic timeout after 60 seconds of inactivity.

**Architecture:**

1. **Start Export** (`createDatabaseExport`) - Creates an export session
   - Returns export metadata immediately: `id`, `rangeCount`, `startedAt`, `expiresAt`
   - Creates a temporary export session on the server
   - Session expires after 60 seconds of inactivity (no range requests)

2. **Fetch Chunks** (`getDatabaseExportPart`) - Retrieve ranges while session is active
   - Make requests for different `rangeNumber` values (1 to rangeCount)
   - Each request fetches a specific chunk of the database
   - Each range request resets the 60-second inactivity timer
   - Requests can be made sequentially or in parallel

3. **End Export** (`endDatabaseExport`) - Clean up the export session
   - POST to `/export/{exportId}/end` to explicitly end the export session
   - Releases the compaction barrier and frees resources
   - Optional - session will auto-expire after 60s of inactivity anyway

**Test flow:**

The generated test cases model this as three operations:

1. **Create Export** - Initiates export session, captures `exportId` and `rangeCount`
2. **Get Export Part** - Retrieves a specific chunk using captured `exportId`
3. **End Export** - Ends the export session (optional, will auto-expire)

**Important for SDK implementations:**

- The `createDatabaseExport` operation returns a **standard JSON response** (not streaming)
- SDKs should:
  - POST to create export, get `{id, rangeCount, startedAt, expiresAt}` response
  - Make `getDatabaseExportPart` requests for each range (1 to rangeCount)
  - POST to `/export/{exportId}/end` when done or it will auto-expire after 60s of inactivity
  - Handle 404 errors if the export expires during retrieval

**Example flow:**

```text
Client                          Server
  |                               |
  |-- POST /export ------------->|
  |<-- {id, rangeCount, ...} ----|  (session created, 60s timeout starts)
  |                               |
  |-- GET /export/ID/ranges/1 -->|  (timer resets to 60s)
  |<-- chunk data 1 --------------|
  |-- GET /export/ID/ranges/2 -->|  (timer resets to 60s)
  |<-- chunk data 2 --------------|
  |   ... (sequential/parallel) ...|
  |-- POST /export/ID/end ------->|  (optional cleanup)
  |<-- 204 No Content -----------|
```

**Operations tested:**

1. **Create Database** - Creates a test database (captures `databaseName` and `branchName`)
2. **Write Data** - Inserts test data into the database
3. **Create Export** - Starts the export session (captures `id` as `exportId` and `rangeCount`)
4. **Get Export Part** - Retrieves a specific range of the export data
5. **End Export** - Ends the export session (POST to `/export/{exportId}/end`)

**Test implementation notes:**

- The `getDatabaseExportPart` test requires a `rangeNumber` parameter
- SDKs should use `rangeNumber: 1` as a literal value to test fetching the first chunk
- Production SDKs should iterate from `1` to `rangeCount` to fetch all chunks
- Each range request extends the export session by 60 seconds
- If more than 60 seconds pass without a range request, the export will auto-expire

**Parameters captured:**

- `exportId` - The ID of the created export (from field `id` in JSON response)
- `rangeCount` - Number of chunks available (from response field `rangeCount`)

**Parameters to provide:**

- `rangeNumber` - Use literal value `1` for testing (represents the first chunk, ranges are 1-indexed)

### Database Import Flow

Imports allow uploading SQLite database files in chunks. The test flow is:

1. **Create Import** - Initiates an import session (captures `importId`)
2. **Upload Chunks** - Sends database chunks to complete the import

**Operations tested:**

- `createImport` - Creates an import session
- `createImportChunk` - Uploads chunks with proper `importId` parameter
- `getImport` - Retrieves import status
- `updateImport` - Updates import metadata
- `deleteImport` - Removes an import

**Parameters captured:**

- `importId` - The ID of the created import session

**Request body for imports:**

```json
{
  "databaseName": "test_db_randomhex",
  "chunkCount": 1
}
```

**Request body for import chunks:**

```json
{
  "chunkNumber": 0,
  "data": "base64-encoded-sqlite-data"
}
```

### Special Considerations

1. **Export ranges** - Exports return an array of ranges. The test captures the first range number for subsequent part retrieval.

2. **Import chunks** - Must reference a valid `importId` from a previously created import.

3. **Prerequisites** - The generator automatically adds prerequisite steps for:
   - `DatabaseExportPart` operations (requires database, data, and export)
   - `ImportChunk` operations (requires import session)

4. **Parameter propagation** - Captured parameters flow through test steps:
   - `databaseName`, `branchName` → from database creation
   - `exportId`, `rangeNumber` → from export creation
   - `importId` → from import creation

### Testing Import/Export Integration

For end-to-end testing of import/export functionality:

1. Create an export from a database with known data
2. Download all export parts
3. Create a new import session
4. Upload the export data as import chunks
5. Verify the imported database matches the source

This flow ensures both export and import mechanisms work correctly and are compatible with each other.
