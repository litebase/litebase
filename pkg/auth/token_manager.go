package auth

import (
	"crypto/rand"
	"fmt"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
)

type TokenManager struct {
	auth         *Auth
	mutex        *sync.Mutex
	tokenStorage TokenStorage
}

type TokenStorage interface {
	Delete(id string) error
	Get(id string) (*Token, error)
	List() ([]*Token, error)
	Store(token *Token) error
	Update(token *Token) error
}

// Create a new instance of a TokenManager.
func NewTokenManager(tokenStorage TokenStorage, auth *Auth) *TokenManager {
	return &TokenManager{
		auth:         auth,
		mutex:        &sync.Mutex{},
		tokenStorage: tokenStorage,
	}
}

// Return a token cache key.
func (tm *TokenManager) tokenCacheKey(tokenId string) string {
	return fmt.Sprintf("token:%s", tokenId)
}

// Retrieve all tokens.
func (tm *TokenManager) All() ([]*Token, error) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	return tm.tokenStorage.List()
}

// Retrieve all tokens.
func (tm *TokenManager) AllTokens() ([]*Token, error) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	return tm.tokenStorage.List()
}

// Retrieve all token IDs.
func (tm *TokenManager) AllTokenIDs() ([]string, error) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	tokens, err := tm.tokenStorage.List()

	if err != nil {
		return nil, err
	}

	var tokenIDs []string

	for _, token := range tokens {
		tokenIDs = append(tokenIDs, token.TokenID)
	}

	return tokenIDs, nil
}

// Create a new token.
func (tm *TokenManager) Create(description string, statements []Statement) (*Token, error) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	tokenID, err := tm.GenerateTokenID()

	if err != nil {
		return nil, err
	}

	tokenSecret := tm.GenerateTokenSecret()

	// Bcrypt the secret
	tokenHash, err := bcrypt.GenerateFromPassword([]byte(tokenSecret), bcrypt.DefaultCost)

	if err != nil {
		return nil, err
	}

	token := NewToken(tm, tokenID, tokenSecret, string(tokenHash), description, statements)

	if err := tm.tokenStorage.Store(token); err != nil {
		return nil, err
	}

	return token, nil
}

// Generate a random token secret.
func (tm *TokenManager) GenerateTokenSecret() string {
	dictionary := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	result := make([]byte, 32)
	for i := range result {
		randomBytes := make([]byte, 1)

		_, err := rand.Read(randomBytes)

		if err != nil {
			// If crypto/rand fails, this is a serious issue
			panic(fmt.Sprintf("crypto/rand failed: %v", err))
		}

		index := int(randomBytes[0]) % len(dictionary)
		result[i] = dictionary[index]
	}

	return string(result)
}

// Generate a unique token ID.
func (tm *TokenManager) GenerateTokenID() (string, error) {
	var (
		rounds    = 0
		maxRounds = 100
	)

	prefix := "lbtk_"
	dictionary := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var tokenID string

	// Generate a random token ID, a-zA-Z1-9
	for {
		result := make([]byte, 32)

		for i := range result {
			randomBytes := make([]byte, 1)

			_, err := rand.Read(randomBytes)

			if err != nil {
				return "", err
			}

			index := int(randomBytes[0]) % len(dictionary)
			result[i] = dictionary[index]
		}

		tokenID = fmt.Sprintf("%s%s", prefix, result)

		// Check if the token ID already exists
		if existingToken, _ := tm.tokenStorage.Get(tokenID); existingToken == nil {
			break
		}

		rounds++

		if rounds > maxRounds {
			return "", fmt.Errorf("could not generate a unique token ID")
		}
	}

	return tokenID, nil
}

// Get a token by ID.
func (tm *TokenManager) Get(tokenID string) (*Token, error) {
	token := &Token{
		TokenManager: tm,
	}

	value := tm.auth.SecretsManager.cache("map").
		Get(tm.tokenCacheKey(tokenID), token)

	if value != nil {
		return token, nil
	}

	token, err := tm.tokenStorage.Get(tokenID)

	if err != nil {
		return nil, err
	}

	token.TokenManager = tm

	tm.auth.SecretsManager.cache("map").
		Put(tm.tokenCacheKey(tokenID), token, time.Second*300)

	return token, nil
}

// Purge a token from the cache.
func (tm *TokenManager) Purge(tokenID string) error {
	tm.auth.SecretsManager.cache("map").Forget(tm.tokenCacheKey(tokenID))
	tm.auth.SecretsManager.cache("transient").Forget(tm.tokenCacheKey(tokenID))
	tm.auth.Broadcast("token:purge", tokenID)

	return nil
}

// Purge all tokens.
func (tm *TokenManager) PurgeAll() error {
	// Get all token IDs from storage.
	tokenIDs, err := tm.AllTokenIDs()

	if err != nil {
		return err
	}

	for _, tokenID := range tokenIDs {
		err := tm.Purge(tokenID)

		if err != nil {
			return err
		}
	}

	return nil
}
