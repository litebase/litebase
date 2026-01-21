# API & CLI Usage

## REST API

### Authentication Headers

All API requests must include an `Authorization` header with one of:

#### Access Key Authentication

```bash
Authorization: Litebase-HMAC-SHA256 credential=<KEY_ID>,signed_headers=<HEADERS>,signature=<SIGNATURE>
```

Example:

```bash
Authorization: Litebase-HMAC-SHA256 credential=acc_abc123,signed_headers=host;x-litebase-date,signature=sha256Hash...
```

#### Token Authentication

```bash
Authorization: Bearer <TOKEN_VALUE>
```

Example:

```bash
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

#### Basic Authentication

```bash
Authorization: Basic <BASE64_ENCODED_CREDENTIALS>
```

Example:

```bash
Authorization: Basic YWRtaW46cGFzc3dvcmQxMjM=
```

### Creating Access Keys

**Endpoint**: `POST /v1/access-keys`

**Request**:

```json
{
  "description": "Production CI/CD",
  "statements": [
    {
      "effect": "allow",
      "resource": "database:*",
      "actions": ["database:create", "database:query", "database:read"]
    }
  ]
}
```

**Response** (200 OK):

```json
{
  "message": "Access key created successfully",
  "data": {
    "accessKeyId": "acc_abc123def456",
    "accessKeySecret": "secret_xyz789abc123",
    "description": "Production CI/CD",
    "statements": [...],
    "createdAt": "2024-01-21T10:30:00Z"
  }
}
```

⚠️ **Important**: Save the `accessKeySecret` immediately. It's only displayed once.

### Listing Access Keys

**Endpoint**: `GET /v1/access-keys`

**Response** (200 OK):

```json
{
  "message": "Access keys retrieved successfully",
  "data": [
    {
      "accessKeyId": "acc_abc123def456",
      "description": "Production CI/CD",
      "statements": [...],
      "createdAt": "2024-01-21T10:30:00Z",
      "updatedAt": "2024-01-21T10:35:00Z"
    }
  ]
}
```

### Getting an Access Key

**Endpoint**: `GET /v1/access-keys/{accessKeyId}`

**Response** (200 OK):

```json
{
  "message": "Access key retrieved successfully",
  "data": {
    "accessKeyId": "acc_abc123def456",
    "description": "Production CI/CD",
    "statements": [...],
    "createdAt": "2024-01-21T10:30:00Z"
  }
}
```

### Updating an Access Key

**Endpoint**: `PUT /v1/access-keys/{accessKeyId}`

**Request**:

```json
{
  "description": "Updated description",
  "statements": [
    {
      "effect": "allow",
      "resource": "database:prod",
      "actions": ["database:read"]
    }
  ]
}
```

**Response** (200 OK):

```json
{
  "message": "Access key updated successfully",
  "data": { ... }
}
```

### Deleting an Access Key

**Endpoint**: `DELETE /v1/access-keys/{accessKeyId}`

**Response** (200 OK):

```json
{
  "message": "Access key deleted successfully",
  "data": {}
}
```

### Similar Endpoints

- **Tokens**: `POST /v1/tokens`, `GET /v1/tokens`, etc.
- **Users**: `POST /v1/users`, `GET /v1/users`, etc.

## CLI Commands

### Access Keys

#### Access Keys: Create

```bash
litebase access-key create \
  --description "CI/CD Pipeline" \
  --statement '{
    "effect": "allow",
    "resource": "database:*",
    "actions": ["read", "write", "query"]
  }'
```

#### List

```bash
litebase access-key list

# Output:
# ┌──────────────────────┬──────────────────────┬──────────────────────────┐
# │ Access Key ID        │ Description          │ Created At               │
# ├──────────────────────┼──────────────────────┼──────────────────────────┤
# │ acc_abc123def456     │ CI/CD Pipeline       │ 2024-01-21 10:30:00      │
# │ acc_xyz789abc123     │ Mobile App           │ 2024-01-20 15:45:00      │
# └──────────────────────┴──────────────────────┴──────────────────────────┘
```

#### Access Keys: Show

```bash
litebase access-key show <access-key-id>
```

#### Access Keys: Update

```bash
litebase access-key update <access-key-id> \
  --description "Updated description" \
  --statement '{
    "effect": "allow",
    "resource": "database:prod",
    "actions": ["read", "write"]
  }'
```

#### Access Keys: Delete

```bash
litebase access-key delete <access-key-id>
```

### Tokens

### Token: Create

```bash
litebase token create \
  --description "Web Application" \
  --statement '{
    "effect": "allow",
    "resource": "database:*",
    "actions": ["read", "query"]
  }'
```

#### Access Keys: List

```bash
litebase token list
```

### Token: Show

```bash
litebase token show <token-id>
```

### Token: Update

```bash
litebase token update <token-id> \
  --description "Updated description"
```

### Token: Delete

```bash
litebase token delete <token-id>
```

### Users

### User: Create

```bash
litebase user create \
  --username alice \
  --password "SecurePassword123!" \
  --description "Database administrator" \
  --statement '{
    "effect": "allow",
    "resource": "*",
    "actions": ["*"]
  }'
```

### User: List

```bash
litebase user list
```

### User: Show

```bash
litebase user show <username>
```

### User: Update

```bash
# Update password
litebase user update alice \
  --password "NewPassword456!"

# Update description
litebase user update alice \
  --description "Senior DBA"

# Update permissions
litebase user update alice \
  --statement '{
    "effect": "allow",
    "resource": "database:*",
    "actions": ["read"]
  }'
```

### User: Delete

```bash
litebase user delete alice
```

## Configuration Profiles

CLI supports multiple profiles for different environments:

### Creating a Profile

```bash
# Profile with access key
litebase profile create prod \
  --url https://api.prod.litebase.com \
  --access-key-id acc_abc123 \
  --access-key-secret secret_xyz789

# Profile with token
litebase profile create staging \
  --url https://api.staging.litebase.com \
  --token eyJhbGc...

# Profile with basic auth
litebase profile create dev \
  --url http://localhost:8080 \
  --username alice \
  --password password123
```

### Using Profiles

```bash
# Use specific profile
litebase --profile prod database list

# Set default profile
litebase profile set-default prod

# Verify current profile
litebase profile current
```

## Examples

### Complete Workflow

```bash
# 1. Create access key for service
KEY=$(litebase access-key create \
  --description "Analytics Service" \
  --statement '{
    "effect": "allow",
    "resource": "database:analytics",
    "actions": ["read", "query"]
  }' | jq -r '.data.accessKeyId')

echo "Created access key: $KEY"

# 2. Export for use in application
export LITEBASE_ACCESS_KEY_ID=$KEY
export LITEBASE_ACCESS_KEY_SECRET=<secret_from_output>

# 3. Verify permissions
litebase access-key show $KEY

# 4. Test with service
curl -H "Authorization: Litebase-HMAC-SHA256 credential=$KEY,..." \
  https://api.litebase.com/v1/databases

# 5. Update permissions if needed
litebase access-key update $KEY \
  --statement '{
    "effect": "allow",
    "resource": "database:*",
    "actions": ["read"]
  }'

```

### Multi-Environment Setup

```bash
# Production
litebase access-key create \
  --description "Prod API Service" \
  --statement '{
    "effect": "allow",
    "resource": "database:prod",
    "actions": ["read", "write", "query"]
  }'

# Staging
litebase access-key create \
  --description "Staging API Service" \
  --statement '{
    "effect": "allow",
    "resource": "database:staging",
    "actions": ["*"]
  }'

# Development
litebase access-key create \
  --description "Dev API Service" \
  --statement '{
    "effect": "allow",
    "resource": "database:dev",
    "actions": ["*"]
  }'
```

## Error Handling

### Common Errors

#### Invalid Credentials

```json
{
  "message": "Unauthorized",
  "data": null
}
```

- Verify Authorization header format
- Check credential ID and secret
- Ensure credential hasn't been deleted

#### Insufficient Permissions

```json
{
  "message": "Forbidden: insufficient permissions",
  "data": null
}
```

- Check credential's statements
- Verify requested resource matches statement resource
- Ensure action is in statement's actions list

#### Invalid Statement Format

```json
{
  "message": "Invalid statement: resource and actions are required",
  "data": null
}
```

- Verify JSON syntax
- Check effect is "allow" or "deny"
- Ensure resource is a valid path
- Verify actions is an array

## Best Practices

1. **Rotate Credentials Regularly**

   ```bash
   # Monthly rotation
   litebase access-key create
   litebase access-key delete <old-key-id>
   ```

2. **Use Different Credentials per Service**

   ```bash
   # Each microservice gets its own credential
   litebase access-key create --description "Auth Service"
   litebase access-key create --description "Payment Service"
   ```

3. **Audit Credential Usage**

   ```bash
   # List all active credentials
   litebase access-key list
   litebase token list
   litebase user list
   ```

4. **Store Credentials Securely**
   - Use environment variables, not config files
   - Use secrets management tools (Vault, SecretsManager)
   - Never commit to version control

5. **Test Permissions Before Production**

   ```bash
   # Create test credential with same permissions
   litebase access-key create --description "Test"

   # Verify it can do what's expected
   litebase --access-key-id <test-key> database query test

   # Delete test credential
   litebase access-key delete <test-key>
   ```
