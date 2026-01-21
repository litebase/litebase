# Credentials

Litebase supports three authentication methods, each optimized for
different use cases.

## Access Keys

Access Keys use HMAC-SHA256 request signing for secure API authentication.

### Access Key: How They Work

1. **Creation**: Generates a unique `AccessKeyID` and `AccessKeySecret`
2. **Request Signing**: Client includes HMAC-SHA256 signature in `Authorization`
   header
3. **Verification**: Server validates signature using stored secret

### Access Key: Structure

```go
type AccessKey struct {
    AccessKeyID     string      // Public identifier
    AccessKeySecret string      // Secret (shown only at creation)
    Description     string      // User-provided label
    Statements      []Statement // Permission grants
    CreatedAt       time.Time
    UpdatedAt       time.Time
}
```

### Access Key: Authorization Header

```bash
Authorization: Litebase-HMAC-SHA256 credential=<ID>,signed_headers=<headers>,signature=<sig>
```

- `credential`: Access Key ID
- `signed_headers`: Comma-separated list of signed headers
- `signature`: HMAC-SHA256 hash of request

### Access Key: CLI Usage

```bash
# Create an access key
litebase access-key create \
  --description "CI/CD Pipeline" \
  --statement '{
    "effect": "allow",
    "resource": "database:*",
    "actions": ["read", "write", "query"]
  }'

# List access keys
litebase access-key list

# Update permissions
litebase access-key update <key-id> \
  --description "Updated label"

# Delete
litebase access-key delete <key-id>
```

### Access Key: API Usage

Access Keys are typically used programmatically. The client library
automatically handles signing:

```bash
# Using curl with signature generation
curl -H "Authorization: Litebase-HMAC-SHA256 credential=acc_123,\
signed_headers=host,signature=..." https://api.litebase.com/v1/databases

# Using a client library (handles signing automatically)
client := litebase.NewClient(
    litebase.WithAccessKey("acc_123", "secret_xyz"),
)
databases, err := client.Databases.List()
```

### Access Key: Security Considerations

- **Secrets displayed once**: Store immediately after creation
- **Rotation**: Generate new key before deactivating old one
- **Expiration**: Not supported; delete and create new key if needed
- **Scope**: Narrow statements to minimum required permissions

## Tokens

Tokens are Bearer-style credentials with hash-based verification.

### Token: How They Work

1. **Creation**: Generates unique `TokenID` and plaintext token value
2. **Storage**: Server stores SHA256 hash of token (never plaintext)
3. **Authentication**: Client sends token in `Authorization: Bearer <token>` header
4. **Verification**: Server compares hash of provided token

### Token: Structure

```go
type Token struct {
    TokenID     string      // Unique identifier
    TokenHash   string      // SHA256 of actual token (stored only)
    TokenSecret string      // Actual token value (shown at creation only)
    Statements  []Statement // Permission grants
    Description string      // User-provided label
    CreatedAt   time.Time
    UpdatedAt   time.Time
}
```

### Token: Authorization Header

```bash
Authorization: Bearer <token_value>
```

### Token: CLI Usage

```bash
# Create a token
litebase token create \
  --description "Web App Token" \
  --statement '{
    "effect": "allow",
    "resource": "database:prod",
    "actions": ["read", "query"]
  }'

# List tokens (doesn't show actual token values)
litebase token list

# Update permissions
litebase token update <token-id> \
  --description "Updated label"

# Delete
litebase token delete <token-id>
```

### Token: API Usage

```bash
# Send token in Authorization header
curl -H "Authorization: Bearer eyJhbGc..." \
  https://api.litebase.com/v1/databases

# Using a client library
client := litebase.NewClient(
    litebase.WithToken("eyJhbGc..."),
)
result, err := client.Database("prod").Query("SELECT * FROM users")
```

### Token: Security Considerations

- **Store securely**: Treat like passwords
- **Single display**: Only shown at creation time
- **No plaintext storage**: Server stores hash only
- **Revocation**: Delete token to immediately revoke access

## Basic Auth

Basic Auth uses HTTP Basic Authentication with usernames and passwords.

### Basic Auth: How They Work

1. **Creation**: Creates user account with hashed password
2. **Authentication**: Client sends `Authorization: Basic <base64:user:pass>`
3. **Verification**: Server verifies username exists and password matches

### Structure

```go
type User struct {
    Username    string      // Username
    Password    string      // Bcrypt-hashed password
    Description string      // User-provided label
    Statements  []Statement // Permission grants
    CreatedAt   time.Time
    UpdatedAt   time.Time
}
```

### Authorization Header Format

```bash
Authorization: Basic <base64(username:password)>
```

Example:

```bash
Authorization: Basic YWRtaW46cGFzc3dvcmQxMjM=  # admin:password123
```

### CLI Usage

```bash
# Create a user
litebase user create \
  --username alice \
  --password "SecurePassword123!" \
  --statement '{
    "effect": "allow",
    "resource": "database:*",
    "actions": ["read", "query"]
  }'

# List users
litebase user list

# Update password
litebase user update alice \
  --password "NewPassword456!"

# Update permissions
litebase user update alice \
  --statement '{
    "effect": "allow",
    "resource": "database:readonly",
    "actions": ["read", "query"]
  }'

# Delete
litebase user delete alice
```

### API Usage

```bash
# Send credentials in Basic Auth format
curl -u username:password \
  https://api.litebase.com/v1/databases

# Using a client library
client := litebase.NewClient(
    litebase.WithBasicAuth("username", "password"),
)
result, err := client.Database("prod").Query("SELECT * FROM users")
```

### Basic Auth: Security Considerations

- **Password strength**: Enforce minimum complexity
- **HTTPS required**: Always use encryption in transit
- **No storage**: Don't send in URLs or unencrypted channels
- **Interactive use**: Best for user-facing applications

## Credential Comparison

| Feature        | Access Key     | Token          | Basic Auth      |
| -------------- | -------------- | -------------- | --------------- |
| Use Case       | Programmatic   | Web apps       | Interactive     |
| Secret Display | Once at create | Once at create | User-managed    |
| Expiration     | Never (rotate) | Never (rotate) | Manual change   |
| Signature      | Yes (HMAC)     | No (Bearer)    | No (Basic)      |
| Password Reset | Rotate         | Rotate         | Change password |
| Best For       | CI/CD, servers | Web browsers   | CLI, UI login   |
| Revocation     | Immediate      | Immediate      | Immediate       |

## Credential Rotation

All credential types support rotation:

## Credential Selection

Choose based on your use case:

- **Access Keys**: Server-to-server communication, CI/CD pipelines, automated tools — lowest latency; HMAC-SHA256 signature verification avoids expensive password-hash checks, making it fastest for high-throughput programmatic use.
- **Tokens**: Web applications, mobile apps, delegated temporary access — server stores SHA256 hashes; verification involves hash comparison.
- **Basic Auth**: Interactive CLI, user login interfaces, legacy systems — user-managed passwords (bcrypt), higher CPU cost for verification; best for interactive use.
