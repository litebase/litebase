package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"
)

type Token struct {
	ID          int64       `json:"id"`
	TokenID     string      `json:"tokenId"`
	TokenHash   string      `json:"tokenHash"`
	TokenSecret string      `json:"tokenSecret,omitempty"`
	Statements  []Statement `json:"statements"`
	Description string      `json:"description"`
	CreatedAt   time.Time   `json:"createdAt"`
	UpdatedAt   time.Time   `json:"updatedAt"`

	TokenManager *TokenManager `json:"-"`
}

type TokenResponse struct {
	TokenID     string      `json:"tokenId"`
	Token       string      `json:"token"`
	Statements  []Statement `json:"statements"`
	Description string      `json:"description"`
	CreatedAt   time.Time   `json:"createdAt"`
	UpdatedAt   time.Time   `json:"updatedAt"`
}

// Create a new instance of a token
func NewToken(
	tokenManager *TokenManager,
	tokenID string,
	tokenSecret string,
	tokenHash string,
	description string,
	statements []Statement,
) *Token {
	return &Token{
		TokenID:     tokenID,
		TokenHash:   tokenHash,
		TokenSecret: tokenSecret,
		Statements:  statements,
		Description: description,
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),

		TokenManager: tokenManager,
	}
}

// Authenticate the token using the provided secret.
func (t *Token) Authenticate(secret string) bool {
	// Use sha256 to hash the provided secret and compare with stored hash
	hashedSecret := sha256.Sum256([]byte(secret))
	hashedSecretHex := hex.EncodeToString(hashedSecret[:])

	return subtle.ConstantTimeCompare([]byte(t.TokenHash), []byte(hashedSecretHex)) == 1
}

// Delete a token.
func (t *Token) Delete() error {
	err := t.TokenManager.tokenStorage.Delete(t.TokenID)

	if err != nil {
		return err
	}

	err = t.TokenManager.Purge(t.TokenID)

	if err != nil {
		slog.Error("failed to purge token", "error", err)
	}

	return nil
}

// Determine if the Token has authorization for the given resources and actions.
func (t *Token) AuthorizeForResource(resources []string, actions []Privilege) bool {
	hasAuthorization := false

	for _, action := range actions {
		for _, resource := range resources {
			if Authorized(t.Statements, resource, action) {
				hasAuthorization = true
				break // No need to check further if one action is authorized
			}
		}
	}

	return hasAuthorization
}

// Rotate the token.
func (t *Token) Rotate() error {
	return t.TokenManager.tokenStorage.Update(t)
}

// Return the hash of the Token.
func (t *Token) Hash() string {
	// Assuming TokenHash is already computed and stored.
	return t.TokenHash
}

// Convert the Token to a response object.
func (t *Token) ToResponse() *TokenResponse {
	return &TokenResponse{
		TokenID:     t.TokenID,
		Statements:  t.Statements,
		Description: t.Description,
		CreatedAt:   t.CreatedAt,
		UpdatedAt:   t.UpdatedAt,
	}
}

// Update the Token statements.
func (t *Token) Update(description string, statements []Statement) error {
	t.Description = description
	t.Statements = statements
	t.UpdatedAt = time.Now().UTC()

	err := t.TokenManager.tokenStorage.Update(t)

	if err != nil {
		return err
	}

	return t.TokenManager.Purge(t.TokenID)
}

// Return the token value. The token secret must still be available.
func (t *Token) Value() (string, error) {
	if t == nil {
		return "", fmt.Errorf("token is nil")
	}

	if t.TokenSecret == "" {
		return "", fmt.Errorf("token secret is not recoverable")
	}

	return fmt.Sprintf("%s%s", t.TokenID, t.TokenSecret), nil
}
