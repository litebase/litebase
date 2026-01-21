# Authentication & Authorization Overview

## Architecture

The auth package manages two core functions:

1. **Authentication**: Verifying the identity of a requester through credentials
2. **Authorization**: Determining what authenticated users can access and modify

### Core Components

```
Auth (main coordinator)
├── AccessKeyManager (HMAC-SHA256 signatures)
├── TokenManager (Bearer tokens)
├── UserManager (Basic auth users)
└── SecretsManager (encryption & key management)
```

## Credential Types

Litebase supports three authentication methods:

### 1. Access Keys (HMAC-SHA256)

- ID and Secret pair for programmatic API access
- Request signing using HMAC-SHA256 algorithm
- Long-lived, can be rotated
- Best for: CLI tools, CI/CD pipelines, server-to-server communication

### 2. Tokens (Bearer)

- Opaque token strings with hash-based verification
- Lightweight, suitable for web applications
- Can be revoked independently
- Best for: Web browsers, temporary access, delegated permissions

### 3. Basic Auth (Username/Password)
- Traditional HTTP Basic Authentication
- User accounts with password hashing
- Interactive access
- Best for: User interfaces, interactive CLI sessions

## Authorization Model

### Permission Structure

Permissions use a **resource-action** model:

```
Resource (what)    + Actions (what you can do) = Permission
"database:123"     + ["read", "write"]          = Grant
"access-key:*"     + ["create", "delete"]       = Allow all access keys
```

### Resources & Scopes

Resources follow a hierarchical pattern:

```
*                                                    # All resources
database:*                                          # All databases
database:DATABASE_ID                                # Specific database
database:DATABASE_ID:branch:BRANCH_ID               # Specific branch
database:DATABASE_ID:branch:BRANCH_ID:table:TABLE   # Specific table
```

### Statements

A statement grants or denies actions on resources:

```json
{
  "effect": "allow",
  "resource": "database:*",
  "actions": ["read", "write", "query"]
}
```

Credentials can have multiple statements, and authorization checks if ANY statement permits the action.

## Lifecycle

### Creating Credentials

```
1. Generate unique ID + Secret
2. Hash the secret (AccessKey/Token only)
3. Store with associated Statements
4. Return full credential (secret shown only once)
```

### Authenticating Requests

```
1. Extract credential from request header
2. Validate credential format
3. Lookup credential by ID
4. Verify signature/token validity
5. Load associated statements
```

### Authorizing Actions

```
1. Get credential from authenticated request
2. Extract requested resource & action
3. Check credential's statements against resource
4. Return permit/deny
```

## Key Interfaces

### AccessKeyStorage
Manages persistent storage of access keys:
- `Store(accessKey)` - Create or update
- `Get(id)` - Retrieve by ID
- `List()` - Get all keys
- `Delete(id)` - Remove key
- `Update(accessKey)` - Modify existing key

### TokenStorage
Manages persistent storage of tokens:
- `Store(token)` - Create or update
- `Get(id)` - Retrieve by ID
- `List()` - Get all tokens
- `Delete(id)` - Remove token
- `Update(token)` - Modify existing token

### UserStorage
Manages persistent storage of users:
- `Store(user)` - Create or update
- `Get(username)` - Retrieve by username
- `List()` - Get all users
- `Delete(username)` - Remove user
- `Update(user)` - Modify existing user

## Integration Points

### Request Validation
The [HTTP middleware](../../pkg/http/authentication_middleware.go) validates credentials on every request:

1. **Captures** credential from `Authorization` header
2. **Verifies** signature (Access Keys) or presence (Tokens/Users)
3. **Enforces** timestamp validation (Access Keys only)
4. **Passes** authenticated credential to handlers

### Request Authorization
Handlers use `request.Authorize()` to check permissions:

```go
err := request.Authorize(
    []string{"database:*", "database:123"},  // Acceptable resources
    []auth.Privilege{auth.DatabasePrivilegeRead},  // Required action
)
```

### Secrets Management
The `SecretsManager` handles:
- Master key management and rotation
- Encryption of sensitive data
- Key derivation for HMAC operations
