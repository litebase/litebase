# OpenAPI Test Case Generator

This tool generates comprehensive test cases from the OpenAPI specification for SDK testing.

## Quick Start

Generate test cases from the OpenAPI spec:

```bash
go run cmd/generate_open_api_test/main.go
```

This creates `generated_open_api_test_cases.json` with a complete test suite.

Note: the generator expects a current OpenAPI spec at `api/generated_open_api.json`. If you need to rebuild the spec first, run:

```bash
go run cmd/generate_open_api_spec/main.go
```

## What It Does

The generator analyzes the OpenAPI specification and creates:

1. **Before All Setup**: Authentication and initial resource creation
2. **Multi-Step Test Cases**: Each operation gets realistic test scenarios
3. **Template Variables**: Support for capturing and injecting values between steps
4. **Complete CRUD Flows**: Create → Read → Update → Delete with verification

Additional output detail

- The generated test requests now include a `requestModel` field when the OpenAPI operation's requestBody is a component schema `$ref`. Example: `"requestModel": "TokenStoreRequest"`.
- The OpenAPI generator prefers to register request and response structs as component schemas and reference them with `$ref` in the generated spec. This reduces duplication and makes generated tests easier to map back to Go structs.

## Example Output

### Create Access Key Test

```json
{
  "operationId": "createAccessKey",
  "name": "Test createAccessKey - Create resource",
  "steps": [
    {
      "request": {
        "name": "Create AccessKey",
        "model": "AccessKey",
        "operation": "createAccessKey",
        "method": "POST",
        "body": {
          "description": "Test Access Key",
          "statements": [
            {
              "effect": "allow",
              "resource": "*",
              "actions": ["*"]
            }
          ]
        },
        "requestModel": "AccessKeyStoreRequest",
        "parameters": []
      },
      "response": {
        "statusCode": 201,
        "content": {},
        "captures": ["access_key_id", "access_key_secret"]
      }
    }
  ]
}
```

### Delete Access Key Test (Multi-Step)

```json
{
  "operationId": "deleteAccessKey",
  "name": "Test deleteAccessKey - Create, delete and verify",
  "steps": [
    {
      "request": {
        "name": "Create test AccessKey",
        "model": "AccessKey",
        "operation": "createAccessKey",
        "method": "POST",
        "body": {...},
        "requestModel": "AccessKeyStoreRequest"
      },
      "response": {
        "statusCode": 201,
        "content": {},
        "captures": ["access_key_id"]
      }
    },
    {
      "request": {
        "name": "Delete AccessKey",
        "operation": "deleteAccessKey",
        "method": "DELETE",
        "parameters": ["access_key_id"],
        "body": {}
      },
      "response": {
        "statusCode": 204
      }
    },
    {
      "request": {
        "name": "Verify AccessKey is deleted",
        "operation": "listAccessKeys",
        "method": "GET",
        "parameters": []
      },
      "response": {
        "statusCode": 200
      }
    }
  ]
}
```

## Key Features

### Template Variables

Use `{{variable_name}}` in requests:

- **Paths**: `/v1/access-keys/{{access_key_id}}`
- **Headers**: `"Authorization": "Bearer {{token}}"`
- **Body**: `"parent_id": "{{database_id}}"`

### Variable Capture

Capture response values using JSONPath:

```json
"captures": {
  "access_key_id": "$.access_key_id",
  "token": "$.token",
  "database_name": "$.name"
}
```

### Multi-Step Tests

Operations that require setup get multi-step tests:

- **List**: Create → List → Verify
- **Get**: Create → Get by ID
- **Update**: Create → Update → Verify
- **Delete**: Create → Delete → Verify deletion

## Using in SDKs

### Python Example

```python
import json
from test_runner import TestRunner

runner = TestRunner(
    base_url="http://localhost:8080",
    test_cases_file="generated_open_api_test_cases.json"
)

# Run all tests
runner.run_all_tests()

# Or run specific operations
runner.run_specific_tests([
    "createAccessKey",
    "deleteAccessKey"
])
```

### JavaScript Example

```javascript
const TestRunner = require('./test-runner');

const runner = new TestRunner({
  baseUrl: 'http://localhost:8080',
  testCasesFile: 'generated_open_api_test_cases.json'
});

// Run all tests
await runner.runAllTests();

// Or run specific operations
await runner.runSpecificTests([
  'createAccessKey',
  'deleteAccessKey'
]);
```

## Documentation

- **[Usage Guide](../../docs/open-api/test-generation-usage.md)** - Complete documentation on using the generated tests
- **[Configuration Guide](../../docs/open-api/test-generator-config.md)** - How to customize the generator
- **[SDK Guidelines](../../docs/open-api/sdk-test-generation.md)** - Guidelines for SDK test generation
- **[Example Runner](../../docs/open-api/example_test_runner.py)** - Python test runner implementation

## Test Case Patterns

The generator creates different test patterns based on operation type:

| Operation Type | Test Pattern             | Steps   |
| -------------- | ------------------------ | ------- |
| **List**       | Create → List → Verify   | 2 steps |
| **Create**     | Create → Verify          | 1 step  |
| **Get**        | Create → Get → Verify    | 2 steps |
| **Update**     | Create → Update → Verify | 2 steps |
| **Delete**     | Create → Delete → Verify | 3 steps |

## Customization

Modify `cmd/generate_open_api_test/main.go` to customize:

- **Before All Setup**: Add authentication, test users, etc.
- **Request Bodies**: Customize test data per resource type
- **Captures**: Define what to extract from responses
- **Path Parameters**: Map parameters to variables

Example customization:

```json
{
  "request": {
    "name": "Create AccessKey",
    "model": "AccessKey",
    "operation": "createAccessKey",
    "method": "POST",
    "body": {
      "name": "test-access-key"
    },
    "requestModel": "AccessKeyStoreRequest",
    "parameters": []
  },
  "response": {
    "statusCode": 201,
    "content": {},
    "captures": ["access_key_id", "access_key_secret"]
  }
}
```

## Generated Files

- **`generated_open_api_test_cases.json`** - Complete test suite ready for SDK integration

## Requirements

- Go 1.21 or later
- Valid OpenAPI spec at `api/generated_open_api.json`

## Related Commands

```bash
# Generate OpenAPI spec (if needed)
go run cmd/generate_open_api_spec/main.go

# Run the test generator
go run cmd/generate_open_api_test/main.go

# Validate the output
cat generated_open_api_test_cases.json | jq .
```

## Support

For questions or issues:

- Check the [Usage Guide](../../docs/open-api/test-generation-usage.md)
- See [Example Test Runner](../../docs/open-api/example_test_runner.py)
- Review [Configuration Options](../../docs/open-api/test-generator-config.md)
