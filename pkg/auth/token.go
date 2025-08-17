package auth

import (
	"log/slog"
	"time"
)

type Token struct {
	ID          int64                `json:"id"`
	TokenID     string               `json:"token_id"`
	TokenHash   string               `json:"token_hash"`
	Statements  []AccessKeyStatement `json:"statements"`
	Description string               `json:"description"`
	CreatedAt   time.Time            `json:"created_at"`
	UpdatedAt   time.Time            `json:"updated_at"`

	TokenManager *TokenManager `json:"-"`
}

type TokenResponse struct {
	TokenID     string               `json:"token_id"`
	Statements  []AccessKeyStatement `json:"statements"`
	Description string               `json:"description"`
	CreatedAt   time.Time            `json:"created_at"`
	UpdatedAt   time.Time            `json:"updated_at"`
}

// Create a new instance of a token
func NewToken(
	tokenManager *TokenManager,
	tokenID string,
	tokenHash string,
	description string,
	statements []AccessKeyStatement,
) *Token {
	return &Token{
		TokenID:     tokenID,
		TokenHash:   tokenHash,
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
func (t *Token) Update(description string, statements []AccessKeyStatement) error {
	t.Description = description
	t.Statements = statements
	t.UpdatedAt = time.Now().UTC()

	err := t.TokenManager.tokenStorage.Update(t)

	if err != nil {
		return err
	}

	return t.TokenManager.Purge(t.TokenID)
}
