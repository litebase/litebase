# Dynamic OpenAPI Generation

## Overview

This implementation demonstrates **comprehensive OpenAPI specification generation** for all public Litebase Server controllers. The system analyzes Go source code using advanced AST parsing, cross-package type resolution, and intelligent response pattern detection to automatically generate detailed OpenAPI 3.1.0 specifications without requiring manual metadata definitions.

## Implementation Details

### Core Components

1. **Advanced Generator** (`pkg/openapi/generator.go`)
   - **Cross-package Type Resolution**: Analyzes types from `auth`, `database`, `http`, `config` packages
   - **Pointer Type Support**: Handles complex pointer types like `*database.BranchSettings`
   - **Method Chaining Detection**: Analyzes `.WithMeta()` and `.WithHeader()` response patterns
   - **Comment-based Documentation**: Extracts descriptions from Go comments
   - **Function Call Type Inference**: Resolves return types from function calls like `logs.QueryMetricKeys()`
   - **Schema Reference Normalization**: Ensures consistent schema naming and referencing
   - AST-based source code analysis with full file context
   - Struct type discovery and field extraction
   - Validation tag parsing (`validate:"required,min=3"`)
   - JSON tag processing (`json:"username,omitempty"`)
   - Security requirement detection

2. **Enhanced Route Analysis**
   - Automatic HTTP method and path inference from router reflection
   - **Advanced Response Pattern Detection**: Identifies complex response structures
   - **Meta Fields and Headers**: Detects `.WithMeta()` and `.WithHeader()` method chains
   - Authorization requirement discovery via AST inspection
   - Parameter extraction from function signatures
   - **Array Type Alias Resolution**: Handles custom array types like `[]AccessKeyIndexResponse`

3. **Intelligent Schema Generation**
   - **Cross-package Schema Resolution**: Automatically discovers and references external types
   - **Pointer Type Dereferencing**: Properly handles `*Type` references to schemas
   - Go struct to OpenAPI schema conversion with full property analysis
   - Validation constraint mapping
   - Required field detection from tags
   - **Enhanced Type Mapping**: Comprehensive Go types → OpenAPI types with format specifications
   - **Schema Deduplication**: Normalizes qualified vs simple type names

4. **Modular Architecture**
   - `schemas.go` - Common schema definitions
   - `security_schemes.go` - Authentication scheme definitions  
   - `tags.go` - API tag definitions
   - `types.go` - Type definitions and utilities

### Advanced Analysis Capabilities

#### Cross-Package Type Resolution

- **Package Discovery**: Automatically analyzes `pkg/auth`, `pkg/database`, `pkg/http`, `pkg/config`
- **Qualified Type Names**: Handles `database.BranchSettings`, `auth.Statement` references
- **Dependency Graph**: Builds complete type dependency trees across packages
- **Schema Normalization**: Converts qualified names to consistent schema keys

#### Enhanced Response Pattern Analysis

- **Method Chaining Detection**: Identifies `.WithMeta()` and `.WithHeader()` calls
- **Function Call Resolution**: Resolves `logs.QueryMetricKeys()` → `[]string`
- **Complex Response Structures**: Handles nested objects with meta fields and headers
- **Array Type Aliases**: Processes custom array types with descriptions

#### Pointer Type Support

- **Dereferencing Logic**: `*database.BranchSettings` → references `BranchSettings` schema
- **Schema Reference Generation**: Creates proper `$ref` links to existing schemas
- **Null Safety**: Handles optional pointer fields correctly
- **Cross-package Pointers**: Supports pointers to external package types

#### Router Inspection

- Analyzes all controller routes using router reflection
- Discovers all controller methods across the controllers
- Maps function names to HTTP operations and paths dynamically

#### Handler Function Analysis

- Uses `go/ast` package for deep code analysis with file context
- Extracts function signatures and parameters
- **Enhanced Response Detection**: Identifies complex response patterns and method chains
- Detects authorization calls (`request.Authorize()`)
- **Variable Type Mapping**: Tracks variable types throughout function scope

#### Type Reflection & Annotation Processing

- **Dynamic Type Discovery**: Finds all request/response types across packages
- Extracts JSON field names and validation rules
- Maps Go validation tags to OpenAPI constraints
- **Comment-based Documentation**: Uses Go comments for field descriptions
- Generates complete request body schemas with full type information

#### OpenAPI Specification Generation

- Produces valid OpenAPI 3.1.0 JSON
- Generates 25+ path items with 40+ operations
- **Enhanced Schema Generation**: Creates 50+ comprehensive schemas automatically
- Includes security schemes and response definitions
- **Proper Schema References**: Uses `$ref` for type reuse and consistency

## Generated Results

### API Endpoints

The system automatically generates endpoints for all public controllers with **comprehensive type information**:

- **40+ HTTP operations** across 25+ path items
- **Complete parameter schemas** with validation constraints
- **Detailed response structures** including meta fields and headers
- **Cross-package type references** properly resolved

### Enhanced Request Body Schemas

**Advanced schemas with cross-package types and descriptions:**

```json
{
  "type": "object",
  "description": "Request payload for creating a new database branch",
  "properties": {
    "name": {
      "type": "string", 
      "description": "Branch name for the new database branch"
    },
    "settings": {
      "$ref": "#/components/schemas/BranchSettings"
    }
  },
  "required": ["name"]
}
```

### Advanced Response Schemas

**Complex response structures with meta fields, headers, and pointer types:**

```json
{
  "type": "object",
  "properties": {
    "status": { "type": "string" },
    "message": { "type": "string" },
    "data": {
      "type": "object",
      "properties": {
        "id": { "type": "string" },
        "name": { "type": "string" },
        "settings": {
          "$ref": "#/components/schemas/BranchSettings"
        },
        "created_at": {
          "type": "string",
          "format": "date-time",
          "description": "Creation timestamp",
          "example": "2023-09-20T14:30:00Z"
        }
      }
    },
    "meta": {
      "type": "object", 
      "properties": {
        "keys": {
          "type": "array",
          "items": { "type": "string" }
        }
      }
    }
  }
}
```

### Cross-Package Schema References

**Automatic resolution of external package types:**

```json
{
  "BranchSettings": {
    "type": "object",
    "description": "Database branch configuration settings",
    "properties": {
      "incrementable_backups": {
        "type": "boolean",
        "description": "Enable incremental backup support"
      }
    }
  },
  "Statement": {
    "type": "object", 
    "description": "Authorization statement for access control",
    "properties": {
      "effect": {
        "type": "string",
        "enum": ["allow", "deny"]
      },
      "resource": { "type": "string" },
      "actions": {
        "type": "array",
        "items": { "type": "string" }
      }
    }
  }
}
```

### Security

Three authentication methods automatically detected:

- `AccessKeyAuth` - API key based authentication
- `BasicAuth` - HTTP Basic authentication  
- `TokenAuth` - Bearer token authentication
  
## Usage

### Automatic Generation via GitHub Actions

The OpenAPI specification is **automatically updated** when HTTP controllers change:

```yaml
# Triggers on PR changes to:
# - pkg/http/**
# - cmd/generate_open_api_spec/**  
# - pkg/openapi/**

# Workflow automatically:
# 1. Generates updated OpenAPI spec
# 2. Commits changes to PR branch
# 3. Comments on PR with update details
```

### Manual Generation

```bash
go run cmd/generate_open_api_spec/main.go
```

**Output**: `api/generated_open_api.json` (188KB+ with 50+ schemas)

### View Generated File

```bash
# View the complete generated OpenAPI spec
cat api/generated_open_api.json | jq .

# View just the schemas section
cat api/generated_open_api.json | jq '.components.schemas'

# Check specific controller endpoints
cat api/generated_open_api.json | jq '.paths."/v1/databases/{databaseName}/branches".post'
```

### Testing Generated Specification

```bash
# Run comprehensive test suite
go test ./pkg/openapi -v

# Run HTTP controller tests
go test ./pkg/http -v

# Verify specific functionality
go test ./pkg/openapi -v -run TestResponseSchemaExtraction
go test ./pkg/openapi -v -run TestStringTypeWithEnumDetection
```

## Technology Stack

- **Advanced AST Analysis**: `go/ast`, `go/parser`, `go/token` with cross-package support
- **Package Resolution**: `golang.org/x/tools/go/packages` for complete package analysis
- **Pattern Matching**: Complex regular expressions for tag parsing and method detection
- **Type System**: Enhanced Go reflection with pointer type support
- **Cross-Package Analysis**: Automatic discovery and resolution of external types
- **Schema Normalization**: Intelligent handling of qualified vs simple type names
- **Output Format**: OpenAPI 3.1.0 JSON specification (188KB+)
- **Automated Integration**: GitHub Actions workflow for continuous specification updates
- **Comprehensive Testing**: Full test coverage with 12+ test suites

## Recent Enhancements

### Pointer Type Resolution (Latest)

- ✅ **Fixed `*database.BranchSettings`**: Now properly references BranchSettings schema
- ✅ **Schema Key Normalization**: Handles qualified names consistently
- ✅ **Reference Generation**: Creates proper `$ref` links instead of generic objects

### Method Chaining Support

- ✅ **`.WithMeta()` Detection**: Automatically detects meta field additions
- ✅ **`.WithHeader()` Support**: Identifies header field enhancements
- ✅ **Function Call Resolution**: Resolves `logs.QueryMetricKeys()` to `[]string`

### Cross-Package Analysis

- ✅ **Database Package**: Full `pkg/database` type resolution
- ✅ **Auth Package**: Complete `pkg/auth` schema generation  
- ✅ **Multi-Package Schemas**: Handles complex inter-package dependencies

### Comment-Based Documentation

- ✅ **Type Descriptions**: Extracts Go comments for schema descriptions
- ✅ **Field Documentation**: Uses field comments for property descriptions
- ✅ **Array Type Aliases**: Handles custom array types with descriptions

## Expansion Potential

This fully dynamic system provides the foundation for:

1. **Enhanced Validation**: Support for complex validation rules and custom constraints
2. **Advanced Response Analysis**: Deep analysis of return types and response patterns
3. **Documentation Integration**: Enhanced comment processing for comprehensive API docs
4. **Integration Testing**: Generate test cases and validation from specifications
5. **Client SDK Generation**: Auto-generate strongly-typed client libraries
6. **API Versioning**: Support for multiple API versions and backward compatibility
7. **Performance Optimization**: Caching and incremental generation for large codebases
