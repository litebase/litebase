# Authorization & Permissions

Litebase uses a statement-based permission model allowing fine-grained control over what authenticated users can access and modify.

## Permission Model

### Core Concepts

A permission is defined by:

1. **Resource**: What entity is being accessed (database, branch, table, column, etc.)
2. **Actions**: What operations are allowed (read, write, delete, etc.)
3. **Effect**: Allow or Deny

### Statements

A statement grants or denies a set of actions on a resource:

```json
{
  "effect": "allow",
  "resource": "database:production-db",
  "actions": ["read", "query"]
}
```

Credentials can have multiple statements. Authorization checks if ANY statement permits the requested action.

### Resource Hierarchy

Resources follow a hierarchical scoping pattern:

```text
*                                                      Level 0: Everything
database:*                                            Level 1: All databases
database:prod-db                                      Level 2: Specific database
database:prod-db:branch:*                             Level 3: All branches
database:prod-db:branch:main                          Level 4: Specific branch
database:prod-db:branch:main:table:*                  Level 5: All tables
database:prod-db:branch:main:table:users              Level 6: Specific table
database:prod-db:branch:main:table:users:column:*     Level 7: All columns
database:prod-db:branch:main:table:users:column:email Level 8: Specific column
```

### Wildcards

Resources support wildcard matching:

```text
database:*              # Matches any database ID
database:prod-*         # Matches "prod-db", "prod-backup", etc.
database:prod-db:*      # Matches all resources under this database
```

Actions always support the `*` wildcard:

```json
{
  "effect": "allow",
  "resource": "database:prod-db",
  "actions": ["*"]  # Allow ALL actions on this database
}
```

## Action Types

Actions are grouped by resource type:

### Access Key Actions

```text
access-key:create    # Create new access keys
access-key:read      # View access key details
access-key:update    # Modify permissions
access-key:delete    # Delete access keys
access-key:list      # List access keys
```

### Token Actions

```text
token:create    # Create new tokens
token:read      # View token details
token:update    # Modify permissions
token:delete    # Delete tokens
token:list      # List tokens
```

### Database Actions

```text
database:read               # Read database metadata
database:write              # Modify database
database:delete             # Delete database
database:analyze            # Run ANALYZE command
database:attach             # Attach databases
database:backup             # Create backups
database:pragma             # Execute PRAGMAs
database:query              # Execute queries
database:create_index       # Create indexes
database:alter_table        # Modify tables
database:transaction_commit # Commit transactions
```

## Authorization Checks

### How Authorization Works

When a request arrives:

1. **Credential extraction**: Auth header is parsed
2. **Credential lookup**: ID is resolved to Access Key, Token, or User
3. **Statements loaded**: Permissions associated with credential
4. **Resource matching**: Requested resource is matched against statements
5. **Action verification**: Check if action is permitted by any statement
6. **Result**: Allow or Deny

### Matching Algorithm

For each statement in credential's permission list:

```text
IF effect == "allow"
  AND resource matches requested resource
  AND requested action is in actions list
  THEN grant permission and return

IF effect == "deny"
  AND resource matches requested resource
  AND requested action is in actions list
  THEN deny permission and return

// After checking all statements
RETURN deny (default: least privilege)
```

### Resource Matching

Resources support both exact and prefix matching:

```text
Statement Resource    | Request Resource              | Match?
database:*            | database:prod                 | YES (wildcard)
database:prod         | database:prod:branch:main     | YES (prefix)
database:prod:*       | database:prod:branch:main     | YES (wildcard)
database:prod:branch:* | database:prod:branch:develop | YES (wildcard)
```

## Common Permission Patterns

### Read-Only User

```json
{
  "effect": "allow",
  "resource": "database:*",
  "actions": ["read", "query"]
}
```

Allows reading all databases and executing queries, but not modifying data.

### Database Admin

```json
{
  "effect": "allow",
  "resource": "database:prod-db",
  "actions": ["*"]
}
```

Allows all operations on a specific database.

### Cluster Admin

```json
{
  "effect": "allow",
  "resource": "*",
  "actions": ["*"]
}
```

Allows all operations on all resources.

### CI/CD Pipeline

```json
[
  {
    "effect": "allow",
    "resource": "database:staging",
    "actions": ["read", "write", "query"]
  },
  {
    "effect": "allow",
    "resource": "database:staging:branch:*",
    "actions": ["*"]
  }
]
```

Allows full access to staging database for automated tests and deployments.

### Development Team

```json
[
  {
    "effect": "allow",
    "resource": "database:dev",
    "actions": ["*"]
  },
  {
    "effect": "allow",
    "resource": "database:prod-db",
    "actions": ["read", "query"]
  }
]
```

Allows full access to development database, read-only to production.

### Multi-Tenant SaaS

```json
[
  {
    "effect": "allow",
    "resource": "database:tenant-12345",
    "actions": ["*"]
  }
]
```

Allows full access to tenant's database only.

## API Examples

### Creating a Credential with Permissions

```bash
# Create access key with read-only access
litebase access-key create \
  --description "Analytics API" \
  --statement '{
    "effect": "allow",
    "resource": "database:analytics-db",
    "actions": ["read", "query"]
  }'

# Create token with write access
litebase token create \
  --description "Mobile App" \
  --statement '{
    "effect": "allow",
    "resource": "database:prod-db:branch:main",
    "actions": ["read", "write", "query"]
  }'

# Create user with limited permissions
litebase user create \
  --username developer \
  --password "SecurePass123!" \
  --statement '{
    "effect": "allow",
    "resource": "database:*",
    "actions": ["read", "query"]
  }' \
  --statement '{
    "effect": "allow",
    "resource": "database:dev",
    "actions": ["write"]
  }'
```

### Updating Permissions

```bash
# Add permission to existing access key
litebase access-key update acc-123 \
  --statement '{
    "effect": "allow",
    "resource": "database:backup",
    "actions": ["read"]
  }'

# Replace all permissions
litebase access-key update acc-123 \
  --statements '[
    {
      "effect": "allow",
      "resource": "database:prod",
      "actions": ["*"]
    }
  ]'
```

## Best Practices

### Principle of Least Privilege

Grant only the minimum permissions needed:

```json
// GOOD: Specific resource and actions
{
  "effect": "allow",
  "resource": "database:prod-db",
  "actions": ["read", "query"]
}

// AVOID: Overly broad permissions
{
  "effect": "allow",
  "resource": "*",
  "actions": ["*"]
}
```

### Separate Credentials by Purpose

Use different credentials for different applications:

```bash
# Web application
litebase access-key create --description "Web App"

# Analytics pipeline
litebase access-key create --description "Analytics"

# Mobile app
litebase token create --description "Mobile App"
```

### Review Permissions Regularly

```bash
# List all credentials and their permissions
litebase access-key list
litebase token list
litebase user list

# Audit permission changes
litebase access-key update <id>  # Review before saving
```

### Use Wildcards Strategically

```json
// Good: Wildcard at appropriate level
{
  "effect": "allow",
  "resource": "database:staging:branch:*",
  "actions": ["read", "write"]
}

// Too broad: Everything
{
  "effect": "allow",
  "resource": "*",
  "actions": ["*"]
}
```

## Troubleshooting

### "Permission Denied" Errors

1. Check credential's statements:

   ```bash
   litebase access-key list --id <key-id>
   ```

2. Verify resource matches:
   - Resource in statement matches requested resource
   - Wildcards are properly formatted

3. Check action is included:
   - Requested action is in statement's actions list
   - Or actions contains `["*"]`

### Testing Permissions

Before applying permissions to production, test with a staging credential:

```bash
# Create test token
litebase token create --description "Test"

# Attempt operations that should work
litebase --token <test-token> database query prod

# Attempt operations that should fail
litebase --token <test-token> database delete prod
```

### Authorization Debugging

Enable debug logging to trace authorization decisions:

```bash
litebase --debug access-key list
```

This shows which statements were evaluated and why access was granted or denied.
