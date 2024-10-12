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
	"strings"
	"sync"
	"time"
)

type SecretsManager struct {
	accessKeyManager   *AccessKeyManager
	databaseKeys       map[string]string
	secretStore        map[string]SecretsStore
	secretStoreMutex   sync.RWMutex
	encrypterInstances map[string]*KeyEncrypter
	mutex              sync.RWMutex
}

func (a *Auth) SecretsManager() *SecretsManager {
	if a.secretsManager == nil {
		a.secretsManager = &SecretsManager{
			accessKeyManager:   a.AccessKeyManager(),
			databaseKeys:       make(map[string]string),
			encrypterInstances: make(map[string]*KeyEncrypter),
			mutex:              sync.RWMutex{},
			secretStore:        make(map[string]SecretsStore),
			secretStoreMutex:   sync.RWMutex{},
		}
	}

	return a.secretsManager
}

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

		s.secretStore["file"] = NewFileSecretsStore("litebase/cache")

		s.secretStoreMutex.Unlock()
	}

	s.secretStoreMutex.RLock()
	defer s.secretStoreMutex.RUnlock()

	return s.secretStore[key]
}

func (s *SecretsManager) databaseSettingCacheKey(databaseId, branchId string) string {
	return fmt.Sprintf("database_secret:%s:%s", databaseId, branchId)
}

func (s *SecretsManager) Decrypt(signature string, text string) (map[string]string, error) {
	return s.Encrypter(signature).Decrypt(text)
}

func (s *SecretsManager) DecryptFor(accessKeyId, text, secret string) (string, error) {
	var err error

	if secret == "" {
		secret, err = s.GetAccessKeySecret(accessKeyId)

		if err != nil {
			return "", err
		}
	}

	encrypter := NewEncrypter([]byte(secret))

	return encrypter.Decrypt(text)
}

func (s *SecretsManager) DeleteDatabaseKey(databaseKey string) {
	filePaths := []string{
		GetDatabaseKeyPath(config.Get().Signature, databaseKey),
	}

	if config.Get().SignatureNext != "" {
		filePaths = append(filePaths, GetDatabaseKeyPath(config.Get().SignatureNext, databaseKey))
	}

	for _, filePath := range filePaths {
		err := storage.ObjectFS().Remove(filePath)

		if err != nil {
			log.Println(err)
		}
	}
}

func (s *SecretsManager) Encrypt(signature string, text string) (string, error) {
	return s.Encrypter(EncrypterKey(signature, "")).Encrypt(text)
}

func (s *SecretsManager) EncryptFor(accessKeyId, text string) (string, error) {
	secret, err := s.GetAccessKeySecret(accessKeyId)

	if err != nil {
		return "", err
	}

	encrypter := NewEncrypter([]byte(secret))

	return encrypter.Encrypt(text)
}

func (s *SecretsManager) EncryptForRuntime(databaseId, signature, text string) (string, error) {
	return s.Encrypter(EncrypterKey(signature, databaseId)).Encrypt(text)
}

func (s *SecretsManager) Encrypter(key string) *KeyEncrypter {
	var encrypter *KeyEncrypter
	s.mutex.RLock()
	encrypter, ok := s.encrypterInstances[key]
	s.mutex.RUnlock()

	if !ok {
		s.mutex.Lock()
		params := strings.Split(key, ":")
		signature := params[0]

		if len(params) > 1 {
			s.encrypterInstances[key] = NewKeyEncrypter(s, signature).ForDatabase(params[1])
		} else {
			s.encrypterInstances[key] = NewKeyEncrypter(s, signature)
		}

		encrypter = s.encrypterInstances[key]

		s.mutex.Unlock()
	}

	return encrypter
}

func EncrypterKey(signature, databaseId string) string {
	if databaseId != "" {
		return fmt.Sprintf("%s:%s", signature, databaseId)
	}

	return signature
}

func (s *SecretsManager) FlushTransients() {
	s.cache("transient").Flush()
}

func GetDatabaseKeyPath(signature, key string) string {
	return fmt.Sprintf("%s/%s/%s", Path(signature), "database_keys", key)
}

func GetDatabaseKeysPath(signature string) string {
	return fmt.Sprintf("%s/%s", Path(signature), "database_keys")
}

func (s *SecretsManager) GetPublicKey(signature, databaseId string) (string, error) {
	var publicKey string
	value := s.cache("map").Get(s.publicKeyCacheKey(signature, databaseId), &publicKey)

	if value != nil {
		publicKey, ok := value.(string)

		if ok {
			return publicKey, nil
		}
	}

	fileValue := s.cache("file").Get(s.publicKeyCacheKey(signature, databaseId), &publicKey)

	if fileValue != nil {
		publicKey, ok := fileValue.(string)

		if ok {
			return publicKey, nil
		}
	}

	path := s.SecretsPath(
		config.Get().Signature,
		fmt.Sprintf("public_keys/%s/public_key", databaseId),
	)

	key, err := storage.ObjectFS().ReadFile(path)

	if err != nil {
		log.Println(err)
		return "", err
	}

	decryptedPublicKey, err := s.Decrypt(signature, string(key))

	if err != nil {
		return "", err
	}

	s.cache("map").Put(s.publicKeyCacheKey(signature, databaseId), decryptedPublicKey["value"], time.Second*60)
	s.cache("file").Put(s.publicKeyCacheKey(signature, databaseId), decryptedPublicKey["value"], time.Second*60)

	return decryptedPublicKey["value"], nil
}

func (s *SecretsManager) GetAccessKeySecret(accessKeyId string) (string, error) {
	var secret string
	value := s.cache("transient").Get(fmt.Sprintf("%s:access_key_secret", accessKeyId), &secret)

	if value != nil {
		return value.(string), nil
	}

	accessKey, err := s.accessKeyManager.Get(accessKeyId)

	if err != nil {
		return "", err
	}

	s.cache("transient").Put(fmt.Sprintf("%s:server_secret", accessKeyId), accessKey.AccessKeySecret, time.Second*1)

	return accessKey.AccessKeySecret, nil
}

func (s *SecretsManager) Init() {
	// Ensure the secrets path exists
	if _, err := storage.ObjectFS().Stat(s.SecretsPath(config.Get().Signature, "")); os.IsNotExist(err) {
		err := storage.ObjectFS().MkdirAll(s.SecretsPath(config.Get().Signature, ""), 0755)

		if err != nil {
			log.Fatal(err)
		}
	}

	// Ensure the access keys path exists
	if _, err := storage.ObjectFS().Stat(s.SecretsPath(config.Get().Signature, "access_keys/")); os.IsNotExist(err) {
		err := storage.ObjectFS().MkdirAll(s.SecretsPath(config.Get().Signature, "access_keys/"), 0755)

		if err != nil {
			log.Fatal(err)
		}
	}

	// Ensure the settings path exists
	if _, err := storage.ObjectFS().Stat(s.SecretsPath(config.Get().Signature, "settings/")); os.IsNotExist(err) {
		err := storage.ObjectFS().MkdirAll(s.SecretsPath(config.Get().Signature, "settings/"), 0755)

		if err != nil {
			log.Fatal(err)
		}
	}

	s.PurgeExpiredSecrets()
}

func (s *SecretsManager) publicKeyCacheKey(signature, databaseId string) string {
	return fmt.Sprintf("public_key:%s:%s", signature, databaseId)
}

func (s *SecretsManager) PurgeDatabaseSettings(databaseId string, branchId string) {
	s.cache("map").Forget(s.databaseSettingCacheKey(databaseId, branchId))
	s.cache("transient").Forget(s.databaseSettingCacheKey(databaseId, branchId))
	s.cache("file").Forget(s.databaseSettingCacheKey(databaseId, branchId))
}

func (s *SecretsManager) PurgeExpiredSecrets() {
	// Get all the file names in the litebase directory
	directories, err := storage.ObjectFS().ReadDir("")

	if err != nil {
		log.Println(err)

		return
	}

	// TODO: ignore directories that start with an underscore
	if len(directories) <= 2 {
		return
	}

	for _, directory := range directories {
		// check if the entry is a directory
		if !directory.IsDir {
			continue
		}

		// Check if there is a manifest file
		manifestPath := fmt.Sprintf("%s/manifest.json", directory.Name)

		if _, err := storage.ObjectFS().Stat(manifestPath); os.IsNotExist(err) {
			continue
		}

		// Check if the signature is still valid
		manifest, err := storage.ObjectFS().ReadFile(manifestPath)

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
			err := storage.ObjectFS().RemoveAll(directory.Name)

			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

func (s *SecretsManager) SecretsPath(signature, key string) string {
	return fmt.Sprintf(
		"%s/%s",
		config.SignatureHash(signature),
		key,
	)
}

func (s *SecretsManager) StoreAccessKey(accessKey *AccessKey) error {
	jsonValue, err := json.Marshal(accessKey)

	if err != nil {
		log.Fatal(err)
	}

	encryptedAccessKey, err := s.Encrypt(
		config.Get().Signature,
		string(jsonValue),
	)

	if err != nil {
		log.Fatal(err)
	}

	err = storage.ObjectFS().WriteFile(
		s.SecretsPath(config.Get().Signature, fmt.Sprintf("access_keys/%s", accessKey.AccessKeyId)),
		[]byte(encryptedAccessKey),
		0666,
	)

	if err != nil {
		return err
	}

	return nil
}

func (s *SecretsManager) StoreDatabaseKey(
	databaseKey string,
	databaseId string,
	branchId string,
) {
	filePaths := []string{
		GetDatabaseKeyPath(config.Get().Signature, databaseKey),
	}

	if config.Get().SignatureNext != "" {
		filePaths = append(filePaths, GetDatabaseKeyPath(config.Get().SignatureNext, databaseKey))
	}
	for _, filePath := range filePaths {
		if _, err := storage.ObjectFS().Stat(filepath.Dir(filePath)); os.IsNotExist(err) {
			storage.ObjectFS().MkdirAll(filepath.Dir(filePath), 0700)
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

		err = storage.ObjectFS().WriteFile(filePath, data, 0644)

		if err != nil {
			log.Fatal(err)
		}
	}
}

func (s *SecretsManager) StoreDatabasePublicKey(signature, databaseId, publicKey string) error {
	if _, err := storage.ObjectFS().Stat(s.SecretsPath(signature, fmt.Sprintf("public_keys/%s", databaseId))); os.IsNotExist(err) {
		storage.ObjectFS().MkdirAll(s.SecretsPath(signature, fmt.Sprintf("public_keys/%s", databaseId)), 0755)
	}

	err := storage.ObjectFS().WriteFile(
		s.SecretsPath(signature, fmt.Sprintf("public_keys/%s/public_key", databaseId)),
		[]byte(publicKey),
		0666,
	)

	if err != nil {
		return err
	}

	return nil
}
