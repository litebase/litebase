package auth

import (
	"encoding/json"
	"fmt"
	"litebase/internal/config"
	"litebase/server/file"
	"litebase/server/storage"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type SecretsManager struct {
	auth               *Auth
	config             *config.Config
	databaseKeys       map[string]string
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
	objectFS *storage.FileSystem,
	tmpFS *storage.FileSystem,
) *SecretsManager {
	return &SecretsManager{
		auth:               auth,
		config:             config,
		databaseKeys:       make(map[string]string),
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
	_, hasFileStore := s.secretStore["file"]
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

	if key == "file" && !hasFileStore {
		s.secretStoreMutex.Lock()

		s.secretStore["file"] = NewFileSecretsStore("litebase/cache/", s.TmpFS)

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

// Decrypt the given text using the given signature
func (s *SecretsManager) Decrypt(signature string, data []byte) (DecryptedSecret, error) {
	return s.Encrypter(signature).Decrypt(data)
}

// Decrypt the given text using the given access key id and secret
func (s *SecretsManager) DecryptFor(accessKeyId, accessKeySecret, text string) (DecryptedSecret, error) {
	var err error

	if accessKeySecret == "" {
		accessKeySecret, err = s.GetAccessKeySecret(accessKeyId)

		if err != nil {
			return DecryptedSecret{}, err
		}
	}

	encrypter := NewEncrypter([]byte(accessKeySecret))

	return encrypter.Decrypt(text)
}

// Delete a database key from the SecretsManager.
func (s *SecretsManager) DeleteDatabaseKey(databaseKey string) error {
	filePaths := []string{
		GetDatabaseKeyPath(s.config.Signature, databaseKey),
	}

	if s.config.SignatureNext != "" {
		filePaths = append(filePaths, GetDatabaseKeyPath(s.config.SignatureNext, databaseKey))
	}

	for _, filePath := range filePaths {
		err := s.ObjectFS.Remove(filePath)

		if err != nil {
			log.Println(err)

			return err
		}
	}

	return nil
}

// Encrypt the given text using the given signature
func (s *SecretsManager) Encrypt(signature string, text []byte) ([]byte, error) {
	return s.Encrypter(signature).Encrypt(text)
}

// Encrypt the given text using the given access key id
func (s *SecretsManager) EncryptFor(accessKeyId, text string) (string, error) {
	secret, err := s.GetAccessKeySecret(accessKeyId)

	if err != nil {
		return "", err
	}

	encrypter := NewEncrypter([]byte(secret))

	return encrypter.Encrypt(text)
}

// Get the KeyEncrypter for the given key
func (s *SecretsManager) Encrypter(signature string) *KeyEncrypter {
	var encrypter *KeyEncrypter

	s.mutex.RLock()
	encrypter, ok := s.encrypterInstances[signature]
	s.mutex.RUnlock()

	if !ok {
		s.mutex.Lock()
		s.encrypterInstances[signature] = NewKeyEncrypter(s, signature)
		encrypter = s.encrypterInstances[signature]

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
		return value.(string), nil
	}

	accessKey, err := s.auth.AccessKeyManager.Get(accessKeyId)

	if err != nil {
		return "", err
	}

	s.cache("transient").Put(fmt.Sprintf("%s:server_secret", accessKeyId), accessKey.AccessKeySecret, time.Second*1)

	return accessKey.AccessKeySecret, nil
}

// Initialize the SecretsManager
func (s *SecretsManager) Init() error {
	// Ensure the secrets path exists
	if _, err := s.ObjectFS.Stat(s.SecretsPath(s.config.Signature, "")); os.IsNotExist(err) {
		err := s.ObjectFS.MkdirAll(s.SecretsPath(s.config.Signature, ""), 0755)

		if err != nil {
			return err
		}
	}

	// Ensure the access keys path exists
	if _, err := s.ObjectFS.Stat(s.SecretsPath(s.config.Signature, "access_keys/")); os.IsNotExist(err) {
		err := s.ObjectFS.MkdirAll(s.SecretsPath(s.config.Signature, "access_keys/"), 0755)

		if err != nil {
			return err
		}
	}

	// Ensure the settings path exists
	if _, err := s.ObjectFS.Stat(s.SecretsPath(s.config.Signature, "settings/")); os.IsNotExist(err) {
		err := s.ObjectFS.MkdirAll(s.SecretsPath(s.config.Signature, "settings/"), 0755)

		if err != nil {
			return err
		}
	}

	s.PurgeExpiredSecrets()

	return nil
}

// Get the public key cache key for the given signature and database id
func (s *SecretsManager) publicKeyCacheKey(signature, databaseId string) string {
	return fmt.Sprintf("public_key:%s:%s", signature, databaseId)
}

// Purge database settings for the given database id and branch id from cache
func (s *SecretsManager) PurgeDatabaseSettings(databaseId string, branchId string) error {
	s.cache("map").Forget(s.databaseSettingCacheKey(databaseId, branchId))
	s.cache("transient").Forget(s.databaseSettingCacheKey(databaseId, branchId))
	s.cache("file").Forget(s.databaseSettingCacheKey(databaseId, branchId))

	return nil
}

// Purge expired database settings from cache
func (s *SecretsManager) PurgeExpiredSecrets() error {
	// Get all the file names in the litebase directory
	directories, err := s.ObjectFS.ReadDir("")

	if err != nil {
		log.Println(err)

		return err
	}

	// TODO: ignore directories that start with an underscore
	if len(directories) <= 2 {
		return nil
	}

	for _, directory := range directories {
		// check if the entry is a directory
		if !directory.IsDir() {
			continue
		}

		// Check if there is a manifest file
		manifestPath := fmt.Sprintf("%s/manifest.json", directory.Name())

		if _, err := s.ObjectFS.Stat(manifestPath); os.IsNotExist(err) {
			continue
		}

		// Check if the signature is still valid
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
		rotatedAtTime, err := time.Parse(time.RFC3339, time.Unix(int64(rotatedAt), 0).Format(time.RFC3339))

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

// Get the path for the given signature and key
func (s *SecretsManager) SecretsPath(signature, key string) string {
	return fmt.Sprintf(
		"%s/%s",
		config.SignatureHash(signature),
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
		s.config.Signature,
		jsonValue,
	)

	if err != nil {
		return err
	}

	err = s.ObjectFS.WriteFile(
		s.SecretsPath(s.config.Signature, fmt.Sprintf("access_keys/%s", accessKey.AccessKeyId)),
		[]byte(encryptedAccessKey),
		0644,
	)

	if err != nil {
		return err
	}

	return nil
}

// Store the given database settings in the SecretsManager
func (s *SecretsManager) StoreDatabaseKey(
	databaseKey string,
	databaseId string,
	branchId string,
) error {
	filePaths := []string{
		GetDatabaseKeyPath(s.config.Signature, databaseKey),
	}

	if s.config.SignatureNext != "" {
		filePaths = append(filePaths, GetDatabaseKeyPath(s.config.SignatureNext, databaseKey))
	}

	for _, filePath := range filePaths {
		if _, err := s.ObjectFS.Stat(filepath.Dir(filePath)); os.IsNotExist(err) {
			s.ObjectFS.MkdirAll(filepath.Dir(filePath), 0700)
		}

		data, err := json.Marshal(map[string]string{
			"key":           databaseKey,
			"database_hash": file.DatabaseHash(databaseId, branchId),
			"database_id":   databaseId,
			"branch_id":     branchId,
		})

		if err != nil {
			log.Fatal(err)
		}

		err = s.ObjectFS.WriteFile(filePath, data, 0644)

		if err != nil {
			return err
		}
	}

	return nil
}
