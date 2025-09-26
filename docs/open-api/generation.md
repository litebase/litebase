# Dynamic OpenAPI Generation

## Overview

This implementation demonstrates **OpenAPI specification generation** for all public Litebase Server controllers. The system analyzes Go source code using AST parsing and reflection to automatically generate comprehensive OpenAPI 3.1.0 specifications without requiring manual metadata definitions.

## Implementation Details

### Core Components

1. **Generator** (`pkg/openapi/generator.go`)
   - Unified OpenAPI generation with route reflection
   - AST-based source code analysis
   - Struct type discovery and field extraction
   - Validation tag parsing (`validate:"required,min=3"`)
   - JSON tag processing (`json:"username,omitempty"`)
   - Security requirement detection

2. **Route Analysis**
   - Automatic HTTP method and path inference from router reflection
   - Response pattern detection from helper function calls
   - Authorization requirement discovery via AST inspection
   - Parameter extraction from function signatures

3. **Schema Generation**
   - Go struct to OpenAPI schema conversion
   - Validation constraint mapping
   - Required field detection from tags
   - Type mapping (Go types → OpenAPI types)

4. **Modular Architecture**
   - `schemas.go` - Common schema definitions
   - `security_schemes.go` - Authentication scheme definitions  
   - `tags.go` - API tag definitions
   - `types.go` - Type definitions and utilities

### Analysis Capabilities

#### Router Inspection

- Analyzes all controller routes using router reflection
- Discovers all controller methods across the controllers
- Maps function names to HTTP operations and paths dynamically

#### Handler Function Analysis

- Uses `go/ast` package for deep code analysis
- Extracts function signatures and parameters
- Identifies response patterns from AST nodes
- Detects authorization calls (`request.Authorize()`)

#### Type Reflection & Annotation Processing

- Discovers all request/response types dynamically
- Extracts JSON field names and validation rules
- Maps Go validation tags to OpenAPI constraints
- Generates complete request body schemas

#### OpenAPI Specification Generation

- Produces valid OpenAPI 3.1.0 JSON
- Generates 25+ path items with 40+ operations
- Creates comprehensive schemas automatically
- Includes security schemes and response definitions

## Generated Results

### API Endpoints

The system automatically generates endpoints for all public controllers. These are listed in the generated OpenAPI specification with their respective HTTP methods, paths, and operations.

### Request Body Schemas

**Example schemas are automatically generated for all controllers:**

```json
{
  "type": "object",
  "properties": {
    "username": { "type": "string" },
    "password": { "type": "string" },
    "statements": { "type": "array" },
    "description": { "type": "string" }
  },
  "required": ["username", "password", "statements"]
}
```

### Response Schemas

Response schemas are inferred from controller return types and documented in the specification.

```json
{
  "type": "object",
  "properties": {
    "id": { "type": "string" },
    "username": { "type": "string" },
    "createdAt": { "type": "string", "format": "date-time" }
  },
  "required": ["id", "username", "createdAt"]
}
```

### Security

Three authentication methods automatically detected:

- `AccessKeyAuth` - API key based authentication
- `BasicAuth` - HTTP Basic authentication  
- `TokenAuth` - Bearer token authentication
  
## Usage

### Generate Specification

```bash
go run cmd/generate_open_api_spec/main.go
```

### View Generated File

The specification is automatically saved to `api/generated_openapi.json`.

```bash
# View the generated OpenAPI spec
cat api/generated_openapi.json | jq .
```

## Technology Stack

- **AST Analysis**: `go/ast`, `go/parser`, `go/token`
- **Pattern Matching**: Regular expressions for tag parsing  
- **Type System**: Go reflection for struct analysis
- **Output Format**: OpenAPI 3.1.0 JSON specification
- **Modular Design**: Separated concerns across multiple files

## Expansion Potential

This fully dynamic system provides the foundation for:

1. **Enhanced Validation**: Support for complex validation rules
2. **Response Schema Extraction**: Analyze return types for response schemas  
3. **Comment Processing**: Extract documentation from Go comments
4. **Integration Testing**: Generate test cases from specifications
5. **API Client Generation**: Auto-generate client SDKs
