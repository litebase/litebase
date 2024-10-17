package auth

import (
	"bytes"
	"encoding/json"
	"fmt"
	"litebase/internal/config"
	"litebase/server/storage"
	"math/rand"
	"os"
	"slices"
	"sync"
	"time"
)

type AccessKeyManager struct {
	auth     *Auth
	config   *config.Config
	mutex    *sync.Mutex
	objectFS *storage.FileSystem
}

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

func (akm *AccessKeyManager) accessKeyCacheKey(accessKeyId string) string {
	return fmt.Sprintf("access_key:%s", accessKeyId)
}

func (akm *AccessKeyManager) AllAccessKeyIds() ([]string, error) {
	files, err := akm.objectFS.ReadDir(akm.auth.SecretsManager.SecretsPath(akm.config.Signature, "access_keys/"))

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

func (akm *AccessKeyManager) Create() (*AccessKey, error) {
	accessKeyId, err := akm.GenerateAccessKeyId()

	if err != nil {
		return nil, err
	}

	accessKey := NewAccessKey(
		akm,
		accessKeyId,
		akm.GenerateAccessKeySecret(),
		[]*AccessKeyPermission{
			{
				Resource: "*",
				Actions:  []string{"*"},
			},
		},
	)

	akm.auth.SecretsManager.StoreAccessKey(accessKey)

	return accessKey, nil
}

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
			result[i] = dictionary[rand.Intn(len(dictionary))]
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

func (akm *AccessKeyManager) GenerateAccessKeySecret() string {
	prefix := "lbdbaks_"

	dictionary := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	result := make([]byte, 32)

	for i := range result {
		result[i] = dictionary[rand.Intn(len(dictionary))]
	}

	return fmt.Sprintf("%s%s", prefix, result)
}

func (akm *AccessKeyManager) Get(accessKeyId string) (*AccessKey, error) {
	var accessKey = &AccessKey{}
	value := akm.auth.SecretsManager.cache("map").Get(akm.accessKeyCacheKey(accessKeyId), accessKey)

	if value != nil {
		return accessKey, nil
	}

	path := akm.auth.SecretsManager.SecretsPath(akm.config.Signature, fmt.Sprintf("access_keys/%s", accessKeyId))

	fileContents, err := akm.objectFS.ReadFile(path)

	if err != nil {
		return nil, err
	}

	decrypted, err := akm.auth.SecretsManager.Decrypt(akm.config.Signature, string(fileContents))

	if err != nil {
		return nil, err
	}

	err = json.NewDecoder(bytes.NewReader([]byte(decrypted["value"]))).Decode(accessKey)

	if err != nil {
		return nil, err
	}

	akm.auth.SecretsManager.cache("map").Put(akm.accessKeyCacheKey(accessKeyId), accessKey, time.Second*300)
	// akm.secretsManager.cache("file").Put(akm.accessKeyCacheKey(accessKeyId), accessKey, time.Second*60)

	return accessKey, err
}

func (akm *AccessKeyManager) Has(databaseKey, accessKeyId string) bool {
	_, err := akm.Get(accessKeyId)

	return err == nil
}

func (akm *AccessKeyManager) Purge(accessKeyId string) {
	akm.auth.SecretsManager.cache("map").Forget(akm.accessKeyCacheKey(accessKeyId))
	akm.auth.SecretsManager.cache("transient").Forget(akm.accessKeyCacheKey(accessKeyId))
}

func (akm *AccessKeyManager) PurgeAll() {
	// Get all the file names in the access keys directory
	files, err := akm.objectFS.ReadDir(akm.auth.SecretsManager.SecretsPath(akm.config.Signature, "access_keys/"))

	if err != nil {
		return
	}

	for _, file := range files {
		akm.Purge(file.Name())
	}
}
