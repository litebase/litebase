package auth

import (
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/storage"
)

type SecretsManager struct {
	auth               *Auth
	config             *config.Config
	NetworkFS          *storage.FileSystem
	secretStore        map[string]SecretsStore
	secretStoreMutex   sync.RWMutex
	encrypterInstances map[string]*KeyEncrypter
	mutex              sync.RWMutex
	ObjectFS           *storage.FileSystem
	TmpFS              *storage.FileSystem
}

type DecryptedSecret struct {
	Key   string
	Value string
}

// Create a new instance of the SecretsManager
func NewSecretsManager(
	auth *Auth,
	config *config.Config,
	networkFS *storage.FileSystem,
	objectFS *storage.FileSystem,
	tmpFS *storage.FileSystem,
	tmpTieredFS *storage.FileSystem,
) *SecretsManager {
	return &SecretsManager{
		auth:               auth,
		config:             config,
		encrypterInstances: make(map[string]*KeyEncrypter),
		mutex:              sync.RWMutex{},
		ObjectFS:           objectFS,
		secretStore:        make(map[string]SecretsStore),
		secretStoreMutex:   sync.RWMutex{},
		TmpFS:              tmpFS,
	}
}

// Get the cache store for the given key. The SecretsManager uses multiple cache
// stores to provide different levels of caching based on performance.
func (s *SecretsManager) cache(key string) SecretsStore {
	s.secretStoreMutex.RLock()
	_, hasMapStore := s.secretStore["map"]
	_, hasTransientStore := s.secretStore["transient"]
	s.secretStoreMutex.RUnlock()

	if key == "map" && !hasMapStore {
		s.secretStoreMutex.Lock()
		s.secretStore["map"] = NewMapSecretsStore()
		s.secretStoreMutex.Unlock()
	}

	if key == "transient" && !hasTransientStore {
		s.secretStoreMutex.Lock()
		s.secretStore["transient"] = NewMapSecretsStore()
		s.secretStoreMutex.Unlock()
	}

	s.secretStoreMutex.RLock()
	defer s.secretStoreMutex.RUnlock()

	return s.secretStore[key]
}

// Return the cache key for the database settings
func (s *SecretsManager) databaseSettingCacheKey(databaseId, branchId string) string {
	return fmt.Sprintf("database_secret:%s:%s", databaseId, branchId)
}

// Decrypt the given text using the given encryption key
func (s *SecretsManager) Decrypt(encryptionKey string, data []byte) (DecryptedSecret, error) {
	return s.Encrypter(encryptionKey).Decrypt(data)
}

// Encrypt the given data using the given key
func (s *SecretsManager) Encrypt(encryptionKey string, data []byte) ([]byte, error) {
	return s.Encrypter(encryptionKey).Encrypt(data)
}

// Get the KeyEncrypter for the given key
func (s *SecretsManager) Encrypter(encryptionKey string) *KeyEncrypter {
	var encrypter *KeyEncrypter

	s.mutex.RLock()
	encrypter, ok := s.encrypterInstances[encryptionKey]
	s.mutex.RUnlock()

	if !ok {
		s.mutex.Lock()
		s.encrypterInstances[encryptionKey] = NewKeyEncrypter(s, encryptionKey)
		encrypter = s.encrypterInstances[encryptionKey]

		s.mutex.Unlock()
	}

	return encrypter
}

// Flush the transient cache
func (s *SecretsManager) FlushTransients() error {
	return s.cache("transient").Flush()
}

// Get the access key secret for the given access key id
func (s *SecretsManager) GetAccessKeySecret(accessKeyId string) (string, error) {
	var secret string
	value := s.cache("transient").Get(fmt.Sprintf("%s:access_key_secret", accessKeyId), &secret)

	if value != nil {
		return secret, nil
	}

	accessKey, err := s.auth.AccessKeyManager.Get(accessKeyId)

	if err != nil {
		return "", err
	}

	s.cache("transient").Put(fmt.Sprintf("%s:access_key_secret", accessKeyId), accessKey.AccessKeySecret, time.Second*1)
	s.cache("transient").Put(fmt.Sprintf("%s:server_secret", accessKeyId), accessKey.AccessKeySecret, time.Second*1)

	return accessKey.AccessKeySecret, nil
}

// Initialize the SecretsManager
func (s *SecretsManager) Init() error {
	// Ensure the secrets path exists
	if _, err := s.ObjectFS.Stat(s.SecretsPath(s.config.EncryptionKey, "")); os.IsNotExist(err) {
		err := s.ObjectFS.MkdirAll(s.SecretsPath(s.config.EncryptionKey, ""), 0750)

		if err != nil {
			return err
		}
	}

	// Ensure the access keys path exists
	if _, err := s.ObjectFS.Stat(s.SecretsPath(s.config.EncryptionKey, "access_keys/")); os.IsNotExist(err) {
		err := s.ObjectFS.MkdirAll(s.SecretsPath(s.config.EncryptionKey, "access_keys/"), 0750)

		if err != nil {
			return err
		}
	}

	// Ensure the settings path exists
	if _, err := s.ObjectFS.Stat(s.SecretsPath(s.config.EncryptionKey, "settings/")); os.IsNotExist(err) {
		err := s.ObjectFS.MkdirAll(s.SecretsPath(s.config.EncryptionKey, "settings/"), 0750)

		if err != nil {
			return err
		}
	}

	err := s.PurgeExpiredSecrets()

	if err != nil {
		slog.Error("Error purging expired secrets:", "error", err)
	}

	return nil
}

// Purge database settings for the given database id and branch id from cache
func (s *SecretsManager) PurgeDatabaseSettings(databaseId string, branchId string) error {
	s.cache("map").Forget(s.databaseSettingCacheKey(databaseId, branchId))
	s.cache("transient").Forget(s.databaseSettingCacheKey(databaseId, branchId))

	return nil
}

// Purge expired database settings from cache
func (s *SecretsManager) PurgeExpiredSecrets() error {
	// Get all the file names in the litebase directory
	directories, err := s.ObjectFS.ReadDir("/")

	if err != nil {
		log.Println(err)

		return err
	}

	if len(directories) <= 2 {
		return nil
	}

	for _, directory := range directories {
		// check if the entry is a directory
		if !directory.IsDir() {
			continue
		}

		// Ignore directories that start with "_"
		if strings.HasPrefix(directory.Name(), "_") {
			continue
		}

		// Check if there is a manifest file
		manifestPath := fmt.Sprintf("%s/manifest.json", directory.Name())

		if _, err := s.ObjectFS.Stat(manifestPath); os.IsNotExist(err) {
			continue
		}

		// Check if the key is still valid
		manifest, err := s.ObjectFS.ReadFile(manifestPath)

		if err != nil {
			continue
		}

		var jsonManifest map[string]interface{}

		err = json.Unmarshal(manifest, &jsonManifest)

		if err != nil {
			continue
		}

		rotatedAt := int(jsonManifest["rotated_at"].(float64))
		rotatedAtTime, err := time.Parse(time.RFC3339, time.Unix(int64(rotatedAt), 0).UTC().Format(time.RFC3339))

		if err != nil {
			continue
		}

		//Check if rotated at is greater than 24 hours
		if rotatedAt == 0 || time.Since(rotatedAtTime) > 24*time.Hour {
			// Remove the directory
			err := s.ObjectFS.RemoveAll(directory.Name())

			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Get the path for the given encryption key and key
func (s *SecretsManager) SecretsPath(encryptionKey, key string) string {
	return fmt.Sprintf(
		"%s/%s",
		config.EncryptionKeyHash(encryptionKey),
		key,
	)
}

// Store the given access key in the SecretsManager
func (s *SecretsManager) StoreAccessKey(accessKey *AccessKey) error {
	jsonValue, err := json.Marshal(accessKey)

	if err != nil {
		log.Println(err)
		return err
	}

	encryptedAccessKey, err := s.Encrypt(
		s.config.EncryptionKey,
		jsonValue,
	)

	if err != nil {
		log.Println(err)
		return err
	}

	err = s.ObjectFS.WriteFile(
		s.SecretsPath(s.config.EncryptionKey, fmt.Sprintf("access_keys/%s", accessKey.AccessKeyID)),
		[]byte(encryptedAccessKey),
		0600,
	)

	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}
