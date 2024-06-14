package auth

import (
	"encoding/json"
	"fmt"
	"litebase/internal/config"
	"litebase/server/storage"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type SecretsManagerInstance struct {
	databaseKeys       map[string]string
	secretStore        map[string]SecretsStore
	secretStoreMutex   sync.RWMutex
	encrypterInstances map[string]*KeyEncrypter
	mutex              sync.RWMutex
}

var staticSecretsManager *SecretsManagerInstance

func SecretsManager() *SecretsManagerInstance {
	if staticSecretsManager == nil {
		staticSecretsManager = &SecretsManagerInstance{
			databaseKeys:       make(map[string]string),
			encrypterInstances: make(map[string]*KeyEncrypter),
			mutex:              sync.RWMutex{},
			secretStore:        make(map[string]SecretsStore),
			secretStoreMutex:   sync.RWMutex{},
		}
	}

	return staticSecretsManager
}

func (s *SecretsManagerInstance) cache(key string) SecretsStore {
	s.secretStoreMutex.RLock()
	// _, hasFileStore := s.secretStore["file"]
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

	// if key == "file" && !hasFileStore {
	// s.secretStoreMutex.Lock()
	// 	s.secretStore["file"] = NewFileSecretsStore(
	// 		fmt.Sprintf("%s/%s", config.Get().TmpPath, "litebase/cache"),
	// 	)
	// 	s.secretStoreMutex.Unlock()
	// }

	s.secretStoreMutex.RLock()
	defer s.secretStoreMutex.RUnlock()

	return s.secretStore[key]
}

func (s *SecretsManagerInstance) databaseSettingCacheKey(databaseUuid, branchUuid string) string {
	return fmt.Sprintf("database_secret:%s:%s", databaseUuid, branchUuid)
}

func (s *SecretsManagerInstance) Decrypt(signature string, text string) (map[string]string, error) {
	return s.Encrypter(signature).Decrypt(text)
}

func (s *SecretsManagerInstance) DecryptFor(accessKeyId, text, secret string) (string, error) {
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

func (s *SecretsManagerInstance) DeleteDatabaseKey(databaseKey string) {
	filePaths := []string{
		GetDatabaseKeyPath(config.Get().Signature, databaseKey),
	}

	if config.Get().SignatureNext != "" {
		filePaths = append(filePaths, GetDatabaseKeyPath(config.Get().SignatureNext, databaseKey))
	}

	for _, filePath := range filePaths {
		err := storage.FS().Remove(filePath)

		if err != nil {
			log.Println(err)
		}
	}
}

func (s *SecretsManagerInstance) Encrypt(signature string, text string) (string, error) {
	return s.Encrypter(EncrypterKey(signature, "")).Encrypt(text)
}

func (s *SecretsManagerInstance) EncryptFor(accessKeyId, text string) (string, error) {
	secret, err := s.GetAccessKeySecret(accessKeyId)

	if err != nil {
		return "", err
	}

	encrypter := NewEncrypter([]byte(secret))

	return encrypter.Encrypt(text)
}

func (s *SecretsManagerInstance) EncryptForRuntime(databaseUuid, signature, text string) (string, error) {
	return s.Encrypter(EncrypterKey(signature, databaseUuid)).Encrypt(text)
}

func (s *SecretsManagerInstance) Encrypter(key string) *KeyEncrypter {
	var encrypter *KeyEncrypter
	s.mutex.RLock()
	encrypter, ok := s.encrypterInstances[key]
	s.mutex.RUnlock()

	if !ok {
		s.mutex.Lock()
		params := strings.Split(key, ":")
		signature := params[0]

		if len(params) > 1 {
			s.encrypterInstances[key] = NewKeyEncrypter(signature).ForDatabase(params[1])
		} else {
			s.encrypterInstances[key] = NewKeyEncrypter(signature)
		}

		encrypter = s.encrypterInstances[key]

		s.mutex.Unlock()
	}

	return encrypter
}

func EncrypterKey(signature, databaseUuid string) string {
	if databaseUuid != "" {
		return fmt.Sprintf("%s:%s", signature, databaseUuid)
	}

	return signature
}

func (s *SecretsManagerInstance) FlushTransients() {
	s.cache("transient").Flush()
}

func GetDatabaseKeyPath(signature, key string) string {
	return fmt.Sprintf("%s/%s/%s", Path(signature), "database_keys", key)
}

func GetDatabaseKeysPath(signature string) string {
	return fmt.Sprintf("%s/%s", Path(signature), "database_keys")
}

func (s *SecretsManagerInstance) GetPublicKey(signature, databaseUuid string) (string, error) {
	var publicKey string
	value := s.cache("map").Get(s.publicKeyCacheKey(signature, databaseUuid), &publicKey)

	if value != nil {
		publicKey, ok := value.(string)

		if ok {
			return publicKey, nil
		}
	}

	// fileValue := s.cache("file").Get(s.publicKeyCacheKey(signature, databaseUuid))

	// if fileValue != nil {
	// 	publicKey, ok := fileValue.(string)

	// 	if ok {
	// 		return publicKey, nil
	// 	}
	// }

	path := s.SecretsPath(
		config.Get().Signature,
		fmt.Sprintf("public_keys/%s/public_key", databaseUuid),
	)

	key, err := storage.FS().ReadFile(path)

	if err != nil {
		return "", err
	}

	decryptedPublicKey, err := s.Decrypt(signature, string(key))

	if err != nil {
		return "", err
	}

	s.cache("map").Put(s.publicKeyCacheKey(signature, databaseUuid), decryptedPublicKey["value"], time.Second*60)
	s.cache("file").Put(s.publicKeyCacheKey(signature, databaseUuid), decryptedPublicKey["value"], time.Second*60)

	return decryptedPublicKey["value"], nil

}

func (s *SecretsManagerInstance) GetAccessKeySecret(accessKeyId string) (string, error) {
	var secret string
	value := s.cache("transient").Get(fmt.Sprintf("%s:access_key_secret", accessKeyId), &secret)

	if value != nil {
		return value.(string), nil
	}

	accessKey, err := AccessKeyManager().Get(accessKeyId)

	if err != nil {
		return "", err
	}

	s.cache("transient").Put(fmt.Sprintf("%s:server_secret", accessKeyId), accessKey.AccessKeySecret, time.Second*1)

	return accessKey.AccessKeySecret, nil
}

func (s *SecretsManagerInstance) Init() {
	// Ensure the secrets path exists
	if _, err := storage.FS().Stat(s.SecretsPath(config.Get().Signature, "")); os.IsNotExist(err) {
		err := storage.FS().MkdirAll(s.SecretsPath(config.Get().Signature, ""), 0755)

		if err != nil {
			log.Fatal(err)
		}
	}

	// Ensure the access keys path exists
	if _, err := storage.FS().Stat(s.SecretsPath(config.Get().Signature, "access_keys")); os.IsNotExist(err) {
		err := storage.FS().MkdirAll(s.SecretsPath(config.Get().Signature, "access_keys"), 0755)

		if err != nil {
			log.Fatal(err)
		}
	}

	// Ensure the settings path exists
	if _, err := storage.FS().Stat(s.SecretsPath(config.Get().Signature, "settings")); os.IsNotExist(err) {
		err := storage.FS().MkdirAll(s.SecretsPath(config.Get().Signature, "settings"), 0755)

		if err != nil {
			log.Fatal(err)
		}
	}

	s.PurgeExpiredSecrets()
}

func (s *SecretsManagerInstance) publicKeyCacheKey(signature, databaseUuid string) string {
	return fmt.Sprintf("public_key:%s:%s", signature, databaseUuid)
}

func (s *SecretsManagerInstance) PurgeDatabaseSettings(databaseUuid string, branchUuid string) {
	s.cache("map").Forget(s.databaseSettingCacheKey(databaseUuid, branchUuid))
	s.cache("transient").Forget(s.databaseSettingCacheKey(databaseUuid, branchUuid))
	s.cache("file").Forget(s.databaseSettingCacheKey(databaseUuid, branchUuid))
}

func (s *SecretsManagerInstance) PurgeExpiredSecrets() {
	// Get all the file names in the litebase directory
	directories, err := storage.FS().ReadDir(fmt.Sprintf("%s/.litebase", config.Get().DataPath))

	if err != nil {
		log.Println(err)

		return
	}

	if len(directories) <= 2 {
		return
	}

	for _, directory := range directories {
		// check if the entry is a directory
		if !directory.IsDir() {
			continue
		}

		// Check if there is a manifest file
		manifestPath := fmt.Sprintf("%s/.litebase/%s/manifest.json", config.Get().DataPath, directory.Name())

		if _, err := storage.FS().Stat(manifestPath); os.IsNotExist(err) {
			continue
		}

		// Check if the signature is still valid
		manifest, err := storage.FS().ReadFile(manifestPath)

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
			err := storage.FS().RemoveAll(fmt.Sprintf("%s/.litebase/%s", config.Get().DataPath, directory.Name()))

			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

func (s *SecretsManagerInstance) SecretsPath(signature, key string) string {
	return fmt.Sprintf(
		"%s/.litebase/%s/%s",
		config.Get().DataPath,
		config.SignatureHash(signature),
		key,
	)
}

func (s *SecretsManagerInstance) StoreAccessKey(accessKey *AccessKey) error {
	jsonValue, err := json.Marshal(accessKey)

	if err != nil {
		log.Fatal(err)
	}

	encryptedAccessKey, err := SecretsManager().Encrypt(
		config.Get().Signature,
		string(jsonValue),
	)

	if err != nil {
		log.Fatal(err)
	}

	err = storage.FS().WriteFile(
		s.SecretsPath(config.Get().Signature, fmt.Sprintf("access_keys/%s", accessKey.AccessKeyId)),
		[]byte(encryptedAccessKey),
		0666,
	)

	if err != nil {
		return err
	}

	return nil
}

func (s *SecretsManagerInstance) StoreDatabaseKey(
	databaseKey string,
	databaseUuid string,
	branchUuid string,
) {
	filePaths := []string{
		GetDatabaseKeyPath(config.Get().Signature, databaseKey),
	}

	if config.Get().SignatureNext != "" {
		filePaths = append(filePaths, GetDatabaseKeyPath(config.Get().SignatureNext, databaseKey))
	}

	for _, filePath := range filePaths {
		if _, err := storage.FS().Stat(filepath.Dir(filePath)); os.IsNotExist(err) {
			storage.FS().MkdirAll(filepath.Dir(filePath), 0700)
		}

		data, _ := json.Marshal(map[string]string{
			"database_uuid": databaseUuid,
			"branch_uuid":   branchUuid,
		})

		err := storage.FS().WriteFile(filePath, data, 0666)

		if err != nil {
			log.Fatal(err)
		}
	}
}

func (s *SecretsManagerInstance) StoreDatabasePublicKey(signature, databaseUuid, publicKey string) error {
	if _, err := storage.FS().Stat(s.SecretsPath(signature, fmt.Sprintf("public_keys/%s", databaseUuid))); os.IsNotExist(err) {
		storage.FS().MkdirAll(s.SecretsPath(signature, fmt.Sprintf("public_keys/%s", databaseUuid)), 0755)
	}

	err := storage.FS().WriteFile(
		s.SecretsPath(signature, fmt.Sprintf("public_keys/%s/public_key", databaseUuid)),
		[]byte(publicKey),
		0666,
	)

	if err != nil {
		return err
	}

	return nil
}
