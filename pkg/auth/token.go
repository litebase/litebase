package auth

import (
	"fmt"
	"log/slog"
	"time"
)

type Token struct {
	ID          int64       `json:"-"`
	TokenID     string      `json:"-"`
	TokenHash   string      `json:"-"`
	TokenSecret string      `json:"-"`
	Statements  []Statement `json:"-"`
	Description string      `json:"-"`
	CreatedAt   time.Time   `json:"-"`
	UpdatedAt   time.Time   `json:"-"`

	TokenManager *TokenManager `json:"-"`
}

type TokenResponse struct {
	TokenID     string      `json:"token_id"`
	Token       string      `json:"token"`
	Statements  []Statement `json:"statements"`
	Description string      `json:"description"`
	CreatedAt   time.Time   `json:"created_at"`
	UpdatedAt   time.Time   `json:"updated_at"`
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
