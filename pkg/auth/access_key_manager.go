package auth

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/storage"
)

type AccessKeyManager struct {
	auth     *Auth
	config   *config.Config
	mutex    *sync.Mutex
	objectFS *storage.FileSystem
}

// Create a New Access Key Manager
func NewAccessKeyManager(
	auth *Auth,
	config *config.Config,
	objectFS *storage.FileSystem,
) *AccessKeyManager {
	return &AccessKeyManager{
		auth:     auth,
		config:   config,
		mutex:    &sync.Mutex{},
		objectFS: objectFS,
	}
}

// Return an access key cache key
func (akm *AccessKeyManager) accessKeyCacheKey(accessKeyId string) string {
	return fmt.Sprintf("access_key:%s", accessKeyId)
}

// Return all access key ids
func (akm *AccessKeyManager) AllAccessKeyIds() ([]string, error) {
	files, err := akm.objectFS.ReadDir(akm.auth.SecretsManager.SecretsPath(akm.config.EncryptionKey, "access_keys/"))

	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}

		return nil, err
	}

	var accessKeyIds []string

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		accessKeyIds = append(accessKeyIds, file.Name())
	}

	return accessKeyIds, nil
}

// Create a new access key
func (akm *AccessKeyManager) Create(description string, statements []AccessKeyStatement) (*AccessKey, error) {
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

	err = akm.auth.SecretsManager.StoreAccessKey(accessKey)

	if err != nil {
		log.Println(err)
		return nil, err
	}

	return accessKey, nil
}

// Generate an access key id
func (akm *AccessKeyManager) GenerateAccessKeyId() (string, error) {
	akm.mutex.Lock()
	defer akm.mutex.Unlock()

	var (
		rounds    = 0
		maxRounds = 100
	)

	prefix := "lbdbak_"
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

// Generate an access key secret
func (akm *AccessKeyManager) GenerateAccessKeySecret() string {
	prefix := "lbdbaks_"

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

// Get an access key
func (akm *AccessKeyManager) Get(accessKeyId string) (*AccessKey, error) {
	var accessKey = &AccessKey{
		accessKeyManager: akm,
	}

	value := akm.auth.SecretsManager.cache("map").Get(akm.accessKeyCacheKey(accessKeyId), accessKey)

	if value != nil {
		return accessKey, nil
	}

	path := akm.auth.SecretsManager.SecretsPath(akm.config.EncryptionKey, fmt.Sprintf("access_keys/%s", accessKeyId))

	fileContents, err := akm.objectFS.ReadFile(path)

	if err != nil {
		return nil, err
	}

	decrypted, err := akm.auth.SecretsManager.Decrypt(akm.config.EncryptionKey, fileContents)

	if err != nil {
		log.Println(err)
		return nil, err
	}

	err = json.NewDecoder(bytes.NewReader([]byte(decrypted.Value))).Decode(accessKey)

	if err != nil {
		return nil, err
	}

	akm.auth.SecretsManager.cache("map").Put(akm.accessKeyCacheKey(accessKeyId), accessKey, time.Second*300)

	return accessKey, err
}

// Purge an access key from the cache
func (akm *AccessKeyManager) Purge(accessKeyId string) error {
	akm.auth.SecretsManager.cache("map").Forget(akm.accessKeyCacheKey(accessKeyId))
	akm.auth.SecretsManager.cache("transient").Forget(akm.accessKeyCacheKey(accessKeyId))
	akm.auth.Broadcast("access-key:purge", accessKeyId)

	return nil
}

// Purge all access keys
func (akm *AccessKeyManager) PurgeAll() error {
	// Get all the file names in the access keys directory
	files, err := akm.objectFS.ReadDir(akm.auth.SecretsManager.SecretsPath(akm.config.EncryptionKey, "access_keys/"))

	if err != nil {
		return err
	}

	for _, file := range files {
		err := akm.Purge(file.Name())

		if err != nil {
			return err
		}
	}

	return nil
}
