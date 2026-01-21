# Implementation Details

This document covers the internal implementation of the auth package for
developers working on Litebase itself.

## Package Structure

```text
pkg/auth/
├── auth.go                 # Main Auth coordinator
├── credential.go           # Credential abstraction
├── statement.go            # Permission statement logic
│
├── access_key.go           # Access Key type and methods
├── access_key_manager.go   # Access Key lifecycle manager
├── access_key_privilege.go # Access Key action constants
│
├── token.go                # Token type and methods
├── token_manager.go        # Token lifecycle manager
├── token_privilege.go      # Token action constants
│
├── user.go                 # User type and methods
├── user_manager.go         # User lifecycle manager
│
├── secrets_manager.go      # Encryption and key management
├── key_manager.go          # Master key handling
├── encrypter.go            # Encryption operations
│
├── privilege.go            # Privilege type definition
├── resource.go             # Resource type definition
├── resources.go            # Resource action mapping
│
├── authorized.go           # Authorization check function
└── ... (tests and utilities)

```

## Core Types

### Auth (Coordinator)

```go
type Auth struct {
    AccessKeyManager *AccessKeyManager
    Config           *config.Config
    ObjectFS         *storage.FileSystem
    SecretsManager   *SecretsManager
    TmpFS            *storage.FileSystem
    UserManager      *UserManager
    TokenManager     *TokenManager
    broadcaster      func(key string, value string)
}

```

Main entry point for all auth operations. Manages three credential managers and
coordinates credential lookups.

### Credential

```go
type Credential struct {
    accessKey        *AccessKey
    auth             *Auth
    CredentialID     string
    CredentialString string
    hash             [32]byte
    Scheme           string
    SignedHeaders    []string
    token            *Token
    user             *User
}

```

Abstraction representing any authenticated credential. Can contain:

- `accessKey` + `CredentialID` + `CredentialString` + `SignedHeaders` for
  HMAC-SHA256
- `token` + `CredentialID` for Bearer tokens
- `user` + `CredentialID` for Basic auth

### Statement

```go
type Statement struct {
    Effect   StatementEffect  // "allow" or "deny"
    Resource Resource         // Resource path (e.g., "database:*")
    Actions  []Privilege      // Actions (e.g., ["read", "write"])
}

```

Defines what a credential can/cannot do. Credentials can have multiple statements.

### AccessKey

```go
type AccessKey struct {
    ID              int64
    AccessKeyID     string
    AccessKeySecret string      // Raw secret
    Description     string
    Statements      []Statement
    CreatedAt       time.Time
    UpdatedAt       time.Time
    AccessKeyManager *AccessKeyManager
}

```

Represents an HMAC-SHA256 signing credential. Secret is shown only once at creation.

### Token

```go
type Token struct {
    ID          int64
    TokenID     string
    TokenHash   string          // SHA256 of actual token
    TokenSecret string          // Raw token (shown only at creation)
    Statements  []Statement
    Description string
    CreatedAt   time.Time
    UpdatedAt   time.Time
    TokenManager *TokenManager
}

```

Represents a Bearer token credential. Token hash is stored, not plaintext.

### User

```go
type User struct {
    Username    string
    Password    string          // Bcrypt-hashed
    Description string
    Statements  []Statement
    CreatedAt   time.Time
    UpdatedAt   time.Time
}

```

Represents a Basic auth user account with password-based access.

## Manager Interfaces

Each credential type uses a storage interface for persistence:

### AccessKeyStorage

```go
type AccessKeyStorage interface {
    Delete(id string) error
    Get(id string) (*AccessKey, error)
    List() ([]*AccessKey, error)
    Store(accessKey *AccessKey) error
    Update(accessKey *AccessKey) error
    UpdateNext(accessKey *AccessKey) error
}

```

### TokenStorage

```go
type TokenStorage interface {
    Delete(id string) error
    Get(id string) (*Token, error)
    List() ([]*Token, error)
    Store(token *Token) error
    Update(token *Token) error
}

```

### UserStorage

```go
type UserStorage interface {
    Delete(username string) error
    Get(username string) (*User, error)
    List() ([]*User, error)
    Store(user *User) error
    Update(user *User) error
}

```

These are implemented by the database layer via
`NewSystemDatabaseAccessKeyStorage` in `pkg/database`.

## Authentication Flow

### Step 1: Credential Capture

```go
// From request Authorization header
credential := auth.CaptureCredential(app.Auth, authHeader)
// Returns nil if header format is invalid

```

Parses header based on scheme:

- **Litebase-HMAC-SHA256**: Extracts credential ID, signed headers, signature
- **Bearer**: Extracts token value
- **Basic**: Extracts base64-encoded user:pass

### Step 2: Credential Lookup

```go
credential, err := app.Auth.GetCredential(credentialID, scheme)
// Loads AccessKey, Token, or User from storage

```

Retrieves full credential object from storage by ID and scheme.

### Step 3: Credential Validation

```go
// For HMAC-SHA256: Verify signature
// For Bearer: Hash comparison
// For Basic: Password verification

```

**Access Key**:

```go
// Reconstruct signature from request
// Compare with provided signature

```

**Token**:

```go
// Hash provided token
// Compare hash with stored hash

```

**User**:

```go
// Verify password with bcrypt

```

### Step 4: Statement Loading

```go
statements := credential.Statements()
// Returns []Statement from loaded credential

```

## Authorization Flow

### Authorization Check

```go
func Authorized(statements []Statement, resource string,
    action Privilege) bool {
    // Check each statement
    for _, stmt := range statements {
        if stmt.Effect == "allow" && 
           matchesResource(resource, stmt.Resource) &&
           contains(stmt.Actions, action) {
            return true
        }
    }
    return false
}

```

### Resource Matching

```go
// Supports prefix and wildcard matching
"database:*"          matches "database:prod"
"database:prod:*"     matches "database:prod:branch:main"
"database:prod"       matches "database:prod:branch:main" (prefix)

```

## Secrets Management

### SecretsManager

```go
type SecretsManager struct {
    // Manages encryption key lifecycle
    // Encrypts/decrypts sensitive data
    // Handles key rotation
}

```

Used for:
- Encrypting stored secrets (when needed)
- Key derivation for HMAC operations
- Master key management

### Key Encryption

Access Key secrets are stored encrypted:

```go
// At creation
encrypted := secretsManager.Encrypt(masterKey, []byte(accessKeySecret))

// At verification
decrypted := secretsManager.Decrypt(masterKey, encrypted)
hmac := crypto.NewHMAC(decrypted, message)

```

## Request Integration

### HTTP Middleware

The [authentication middleware](../../pkg/http/authentication_middleware.go)
orchestrates the flow:

```go
func Authentication(ctx context.Context, request *Request) (*Request,
    Response) {
    // 1. Capture credential from header
    credential := request.Credential()
    
    // 2. Validate based on type
    switch credential.Type() {
    case auth.CredentialTypeBasicAuth:
        if !basicAuth(request) { return unauthorized }
    case auth.CredentialTypeToken:
        if !tokenAuth(credential) { return unauthorized }
    case auth.CredentialTypeAccessKey:
        if !ensureRequestIsProperlySigned(request) { return unauthorized }
        if !ensureRequestIsNotExpired(request) { return unauthorized }
    }
    
    // 3. Attach to request for authorization checks
    return request, Response{}
}

```

### Request Authorization

Handlers call `request.Authorize()` to check permissions:

```go
func DatabaseShowHandler(ctx context.Context, request *Request) Response {
    databaseID := request.Param("databaseId")
    
    // Authorize: check if credential can read this database
    err := request.Authorize(
        []string{
            "*",
            "database:*",
            "database:" + databaseID,
        },
        []auth.Privilege{auth.DatabasePrivilegeRead},
    )
    
    if err != nil {
        return ForbiddenResponse(err)
    }
    
    // ... process request ...
}

```

## Key Implementation Details

### Unique ID Generation

Access Keys and Tokens use cryptographically secure random generation:

```go
// Generate access key ID
func (akm *AccessKeyManager) GenerateAccessKeyId() (string, error) {
    randomBytes := make([]byte, 16)
    rand.Read(randomBytes)
    return "acc_" + hex.EncodeToString(randomBytes), nil
}

// Generate token
func (tm *TokenManager) GenerateToken() (string, error) {
    randomBytes := make([]byte, 32)
    rand.Read(randomBytes)
    return hex.EncodeToString(randomBytes), nil
}

```

### Password Hashing

User passwords use bcrypt for secure hashing:

```go
// On user creation
bcryptHash := bcrypt.GenerateFromPassword(password, bcrypt.DefaultCost)

// On authentication
bcrypt.CompareHashAndPassword(storedHash, providedPassword)

```

### Token Hashing

Token values are hashed with SHA256 before storage:

```go
// On token creation
tokenHash := sha256.Sum256([]byte(tokenValue))

// On authentication
providedHash := sha256.Sum256([]byte(providedToken))
// Compare hashes instead of plaintext tokens

```

### HMAC-SHA256 Signing
Access Keys use HMAC for request signing:

```go
// Client side: sign request
h := hmac.New(sha256.New, []byte(accessKeySecret))
h.Write(stringToSign)
signature := hex.EncodeToString(h.Sum(nil))

// Server side: verify signature
h := hmac.New(sha256.New, []byte(storedSecret))
h.Write(stringToSign)
expectedSignature := hex.EncodeToString(h.Sum(nil))
// Compare signatures

```

## Testing

### Test Utilities
The auth package is heavily tested using `test.RunWithApp()`:

```go
func TestAccess(t *testing.T) {
    test.RunWithApp(t, func(app *server.App) {
        // Auth system is fully initialized
        // Managers have storage backed by system database
        
        accessKey := app.Auth.AccessKeyManager.Create(...)
        credential := app.Auth.GetCredential(...)
        authorized := auth.Authorized(statements, resource, action)
    })
}

```

### Storage Implementation
In tests and production, storage is implemented by the database layer:

```go
// Actual storage in system database
type SystemDatabaseAccessKeyStorage struct {
    systemDatabase *database.Database
}

func (s *SystemDatabaseAccessKeyStorage) Store(ak *AccessKey) error {
    // INSERT or REPLACE into system database tables
}

func (s *SystemDatabaseAccessKeyStorage) Get(id string) (*AccessKey, error) {
    // SELECT from system database
}

```

## Best Practices for Implementers

1. **Always validate credentials exist before using**: Nil checks after `GetCredential()`
2. **Check authorization in handlers**: Don't rely on routing for security
3. **Log auth events**: Use `app.Auth.Broadcast()` for auditing
4. **Rotate secrets regularly**: Implement lifecycle management
5. **Use existing test utilities**: `test.RunWithApp()` sets up auth fully
6. **Store secrets encrypted**: Use `SecretsManager` for sensitive data
7. **Document permission requirements**: What statements does an API endpoint need?

## Future Enhancements

- Token expiration support
- Audit logging of auth events
- Rate limiting per credential
- IP allowlisting for credentials
- Certificate-based authentication
