package auth

import (
	"encoding/json"
	"fmt"
	"litebasedb/internal/config"
	"litebasedb/server/storage"
	"math/rand"
	"slices"
	"sync"
	"time"
)

type AccessKeyManagerInstance struct {
	mutex *sync.Mutex
}

var accessKeyManagerInstance *AccessKeyManagerInstance

func AccessKeyManager() *AccessKeyManagerInstance {
	if accessKeyManagerInstance == nil {
		accessKeyManagerInstance = &AccessKeyManagerInstance{
			mutex: &sync.Mutex{},
		}
	}

	return accessKeyManagerInstance
}

func (akm *AccessKeyManagerInstance) accessKeyCacheKey(accessKeyId string) string {
	return fmt.Sprintf("access_key:%s", accessKeyId)
}

func (akm *AccessKeyManagerInstance) AllAccessKeyIds() ([]string, error) {
	files, err := storage.FS().ReadDir(SecretsManager().SecretsPath(config.Get().Signature, "access_keys"))

	if err != nil {
		return nil, err
	}

	var accessKeyIds []string

	for _, file := range files {
		accessKeyIds = append(accessKeyIds, file.Name())
	}

	return accessKeyIds, nil
}

func (akm *AccessKeyManagerInstance) Create() (*AccessKey, error) {
	accessKeyId, err := akm.GenerateAccessKeyId()

	if err != nil {
		return nil, err
	}

	accessKey := &AccessKey{
		AccessKeyId:     accessKeyId,
		AccessKeySecret: akm.GenerateAccessKeySecret(),
		Permissions: []*AccessKeyPermission{
			{
				Resource: "*",
				Actions:  []string{"*"},
			},
		},
	}

	SecretsManager().StoreAccessKey(accessKey)

	return accessKey, nil
}

func (akm *AccessKeyManagerInstance) GenerateAccessKeyId() (string, error) {
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

func (akm *AccessKeyManagerInstance) GenerateAccessKeySecret() string {
	prefix := "lbdbaks_"

	dictionary := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	result := make([]byte, 32)

	for i := range result {
		result[i] = dictionary[rand.Intn(len(dictionary))]
	}

	return fmt.Sprintf("%s%s", prefix, result)
}

func (akm *AccessKeyManagerInstance) Get(accessKeyId string) (*AccessKey, error) {
	var accessKey *AccessKey
	value := SecretsManager().cache("map").Get(akm.accessKeyCacheKey(accessKeyId), &AccessKey{})

	if value != nil {
		accessKey, ok := value.(*AccessKey)

		if ok {
			return accessKey, nil
		}
	}

	// fileValue := SecretsManager().cache("file").Get(akm.accessKeyCacheKey(accessKeyId), &AccessKey{})

	// if fileValue != nil {
	// 	json.Unmarshal([]byte(fileValue.(string)), &accessKey)
	// }

	// if accessKey != nil {
	// 	return accessKey, nil
	// }

	path := SecretsManager().SecretsPath(config.Get().Signature, fmt.Sprintf("access_keys/%s", accessKeyId))

	fileContents, err := storage.FS().ReadFile(path)

	if err != nil {
		return nil, err
	}

	decrypted, err := SecretsManager().Decrypt(config.Get().Signature, string(fileContents))

	if err != nil {
		return nil, err
	}

	err = json.Unmarshal([]byte(decrypted["value"]), &accessKey)

	if err != nil {
		return nil, err
	}

	SecretsManager().cache("map").Put(akm.accessKeyCacheKey(accessKeyId), accessKey, time.Second*300)
	// SecretsManager().cache("file").Put(akm.accessKeyCacheKey(accessKeyId), accessKey, time.Second*60)

	return accessKey, nil
}

func (akm *AccessKeyManagerInstance) Has(databaseKey, accessKeyId string) bool {
	_, err := akm.Get(accessKeyId)

	return err == nil
}

func (akm *AccessKeyManagerInstance) Purge(accessKeyId string) {
	SecretsManager().cache("map").Forget(akm.accessKeyCacheKey(accessKeyId))
	SecretsManager().cache("transient").Forget(akm.accessKeyCacheKey(accessKeyId))
}

func (akm *AccessKeyManagerInstance) PurgeAll() {
	// Get all the file names in the access keys directory
	files, err := storage.FS().ReadDir(SecretsManager().SecretsPath(config.Get().Signature, "access_keys"))

	if err != nil {
		return
	}

	for _, file := range files {
		AccessKeyManager().Purge(file.Name())
	}
}
