package auth

import (
	"crypto/rand"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/litebase/litebase/pkg/config"
)

type AccessKeyManager struct {
	accessKeyStorage AccessKeyStorage
	auth             *Auth
	config           *config.Config
	mutex            *sync.Mutex
}

type AccessKeyStorage interface {
	Delete(id string) error
	Get(id string) (*AccessKey, error)
	List() ([]*AccessKey, error)
	Store(accessKey *AccessKey) error
	Update(accessKey *AccessKey) error
	UpdateNext(accessKey *AccessKey) error
}

// Create a new instance of an AccessKeyManager.
func NewAccessKeyManager(
	accessKeyStorage AccessKeyStorage,
	auth *Auth,
	config *config.Config,
) *AccessKeyManager {
	return &AccessKeyManager{
		accessKeyStorage: accessKeyStorage,
		auth:             auth,
		config:           config,
		mutex:            &sync.Mutex{},
	}
}

// Return an access key cache key.
func (akm *AccessKeyManager) accessKeyCacheKey(accessKeyId string) string {
	return fmt.Sprintf("access_key:%s", accessKeyId)
}

// Retrieve all the access keys.
func (akm *AccessKeyManager) All() ([]*AccessKey, error) {
	accessKeys, err := akm.accessKeyStorage.List()

	if err != nil {
		return nil, err
	}

	return accessKeys, nil
}

// Return all access key ids.
func (akm *AccessKeyManager) AllAccessKeyIds() ([]string, error) {
	accessKeys, err := akm.accessKeyStorage.List()

	if err != nil {
		return nil, err
	}

	var accessKeyIds []string

	for _, accessKey := range accessKeys {
		accessKeyIds = append(accessKeyIds, accessKey.AccessKeyID)
	}

	return accessKeyIds, nil
}

// Create a new access key.
func (akm *AccessKeyManager) Create(description string, statements []Statement) (*AccessKey, error) {
	accessKeyId, err := akm.GenerateAccessKeyId()

	if err != nil {
		return nil, err
	}

	accessKey := NewAccessKey(
		akm,
		accessKeyId,
		akm.GenerateAccessKeySecret(),
		description,
		statements,
	)

	err = akm.accessKeyStorage.Store(accessKey)

	if err != nil {
		slog.Error("Error creating access key", "error", err)

		return nil, err
	}

	return accessKey, nil
}

// Generate an access key id.
func (akm *AccessKeyManager) GenerateAccessKeyId() (string, error) {
	akm.mutex.Lock()
	defer akm.mutex.Unlock()

	var (
		rounds    = 0
		maxRounds = 100
	)

	prefix := "lbakid_"
	dictionary := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var accessKeyId string

	// Get all the access key ids
	accessKeyIds, err := akm.AllAccessKeyIds()

	if err != nil {
		return "", err
	}

	// Generate a random access key id, a-zA-z1-9
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

		accessKeyId = fmt.Sprintf("%s%s", prefix, result)

		// Check if the access key id already exists
		if !slices.Contains(accessKeyIds, accessKeyId) {
			return accessKeyId, nil
		}

		rounds++

		if rounds > maxRounds {
			return "", fmt.Errorf("could not generate a unique access key id")
		}
	}
}

// Generate an access key secret.
func (akm *AccessKeyManager) GenerateAccessKeySecret() string {
	prefix := "lbaks_"

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

	return fmt.Sprintf("%s%s", prefix, result)
}

// Get an access key.
func (akm *AccessKeyManager) Get(accessKeyId string) (*AccessKey, error) {
	var accessKey = &AccessKey{
		AccessKeyManager: akm,
	}

	value := akm.auth.SecretsManager.cache("map").
		Get(akm.accessKeyCacheKey(accessKeyId), accessKey)

	if value != nil {
		return accessKey, nil
	}

	accessKey, err := akm.accessKeyStorage.Get(accessKeyId)

	if err != nil {
		slog.Debug("Error getting access key from storage", "error", err)
		return nil, err
	}

	accessKey.AccessKeyManager = akm

	akm.auth.SecretsManager.cache("map").
		Put(akm.accessKeyCacheKey(accessKeyId), accessKey, time.Second*300)

	return accessKey, nil
}

// Purge an access key from the cache.
func (akm *AccessKeyManager) Purge(accessKeyId string) error {
	akm.auth.SecretsManager.cache("map").Forget(akm.accessKeyCacheKey(accessKeyId))
	akm.auth.SecretsManager.cache("transient").Forget(akm.accessKeyCacheKey(accessKeyId))
	akm.auth.Broadcast("access-key:purge", accessKeyId)

	return nil
}

// Purge all access keys.
func (akm *AccessKeyManager) PurgeAll() error {
	// Get all access key IDs from storage
	accessKeyIds, err := akm.AllAccessKeyIds()

	if err != nil {
		return err
	}

	for _, accessKeyId := range accessKeyIds {
		err := akm.Purge(accessKeyId)

		if err != nil {
			return err
		}
	}

	return nil
}
