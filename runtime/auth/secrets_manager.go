package auth

import (
	"encoding/json"
	"fmt"
	"litebasedb/runtime/config"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

type SecretsManagerInstance struct {
	secretStore        map[string]SecretsStore
	encrypterInstances map[string]*KeyEncrypter
	mutex              *sync.Mutex
}

var staticSecretsManager *SecretsManagerInstance

func SecretsManager() *SecretsManagerInstance {
	if staticSecretsManager == nil {
		staticSecretsManager = &SecretsManagerInstance{
			encrypterInstances: make(map[string]*KeyEncrypter),
			mutex:              &sync.Mutex{},
			secretStore:        make(map[string]SecretsStore),
		}
	}

	return staticSecretsManager
}

func (s *SecretsManagerInstance) accessKeyCacheKey(accessKeyId string) string {
	return "access_key:" + accessKeyId
}

func (s *SecretsManagerInstance) cache(key string) SecretsStore {
	_, hasFileStore := s.secretStore["file"]
	_, hasMapStore := s.secretStore["map"]
	_, hasTransientStore := s.secretStore["transient"]

	if key == "map" && !hasMapStore {
		s.secretStore["map"] = NewMapSecretsStore()
	}

	if key == "transient" && !hasTransientStore {
		s.secretStore["transient"] = NewMapSecretsStore()
	}

	if key == "file" && !hasFileStore {
		s.secretStore["file"] = NewFileSecretsStore(
			config.Get("tmp_path") + "/litebasedb/cache",
		)
	}

	return s.secretStore[key]
}

func (s *SecretsManagerInstance) databaseSettingCacheKey(databaseUuid string, branchUuid string) string {
	return fmt.Sprintf("database_secret:%s:%s", databaseUuid, branchUuid)
}

func (s *SecretsManagerInstance) Decrypt(signature string, text string) (map[string]string, error) {
	return s.Encrypter(signature).Decrypt(text)
}

func (s *SecretsManagerInstance) DecryptFor(accessKeyId string, text string, secret string) (string, error) {
	var err error

	if secret == "" {
		secret, err = s.GetSecret(accessKeyId)

		if err != nil {
			return "", err
		}
	}

	encrypter := NewEncrypter([]byte(secret))

	return encrypter.Decrypt(text)
}

func (s *SecretsManagerInstance) DeleteAccessKey(accessKeyId string) {
	s.PurgeAccessKey(accessKeyId)

	path := s.SecretsPath(fmt.Sprintf("access_keys/%s", accessKeyId))

	os.Remove(path)
}

func (s *SecretsManagerInstance) DeleteSettings(databaseUuid string, branchUuid string) {
	path := s.SecretsPath(fmt.Sprintf("settings/%s", databaseUuid))
	os.RemoveAll(path)
}

func (s *SecretsManagerInstance) Encrypt(signature string, text string) (string, error) {
	return s.Encrypter(signature).Encrypt(text)
}

func (s *SecretsManagerInstance) Encrypter(signature string) *KeyEncrypter {
	if _, ok := s.encrypterInstances[signature]; !ok {
		s.mutex.Lock()
		s.encrypterInstances[signature] = NewKeyEncrypter(signature)
		s.mutex.Unlock()
	}

	return s.encrypterInstances[signature]
}

func (s *SecretsManagerInstance) EncryptFor(accessKeyId, text string) (string, error) {
	secret, err := s.GetSecret(accessKeyId)

	if err != nil {
		return "", err
	}

	encrypter := NewEncrypter([]byte(secret))

	return encrypter.Encrypt(text)
}

func (s *SecretsManagerInstance) FlushTransients() {
	s.cache("transient").Flush()
}

func (s *SecretsManagerInstance) GetAccessKey(accessKeyId string) (*AccessKey, error) {
	var accessKey *AccessKey
	value := s.cache("map").Get(s.accessKeyCacheKey(accessKeyId))

	if value != nil {
		accessKey, ok := value.(*AccessKey)

		if ok {
			return accessKey, nil
		}
	}

	fileValue := s.cache("file").Get(s.accessKeyCacheKey(accessKeyId))

	if fileValue != nil {
		json.Unmarshal([]byte(fileValue.(string)), &accessKey)
	}

	if accessKey != nil {
		return accessKey, nil
	}

	path := s.SecretsPath(fmt.Sprintf("access_keys/%s", accessKeyId))

	fileContents, err := os.ReadFile(path)

	if err != nil {
		return nil, err
	}

	decrypted, err := s.Decrypt(config.Get("signature"), string(fileContents))

	if err != nil {
		return nil, err
	}

	err = json.Unmarshal([]byte(decrypted["value"]), &accessKey)

	if err != nil {
		return nil, err
	}

	s.cache("map").Put(s.accessKeyCacheKey(accessKeyId), accessKey, time.Second*60)
	s.cache("file").Put(s.accessKeyCacheKey(accessKeyId), accessKey, time.Second*60)

	return accessKey, nil
}

func (s *SecretsManagerInstance) GetAwsCredentials(databaseUuid string, branchUuid string) (map[string]string, error) {
	value, err := s.GetDatabaseSettings(databaseUuid, branchUuid)

	if err != nil {
		return nil, err
	}

	if value == nil {
		return nil, nil
	}

	awsAccessToken, hasAwsAccessToken := value["aws_access_token"]

	if hasAwsAccessToken {
		credentials := strings.Split(awsAccessToken.(string), ":")

		return map[string]string{
			"key":    credentials[0],
			"secret": credentials[1],
			"token":  credentials[1],
		}, nil
	}

	return nil, nil
}

func (s *SecretsManagerInstance) GetBackupBucketName(databaseUuid string, branchUuid string) (string, error) {
	value, err := s.GetDatabaseSettings(databaseUuid, branchUuid)

	if err != nil {
		return "", err
	}

	if value == nil {
		return "", nil
	}

	backupBucketName, hasBackupBucketName := value["backupBucketName"]

	if hasBackupBucketName {
		return backupBucketName.(string), nil
	}

	return "", nil
}

func (s *SecretsManagerInstance) GetConnectionKey(databaseUuid string, branchUuid string) (string, error) {
	value, err := s.GetDatabaseSettings(databaseUuid, branchUuid)

	if err != nil {
		return "", err
	}

	if value == nil {
		return "", nil
	}

	connectionKey, hasConnectionKey := value["connectionKey"]

	if hasConnectionKey {
		return connectionKey.(string), nil
	}

	return "", nil
}

func (s *SecretsManagerInstance) GetDatabaseKey(accessKeyId string) (string, error) {
	accessKey, err := s.GetAccessKey(accessKeyId)

	if err != nil {
		return "", err
	}

	return accessKey.GetAccessKeyId(), nil

}

func (s *SecretsManagerInstance) GetDatabaseSettings(databaseUuid string, branchUuid string) (map[string]interface{}, error) {
	value := s.cache("map").Get(s.databaseSettingCacheKey(databaseUuid, branchUuid))

	if value != nil {
		return value.(map[string]interface{}), nil
	}

	value = s.cache("file").Get(s.databaseSettingCacheKey(databaseUuid, branchUuid))

	if value != nil {
		return value.(map[string]interface{}), nil
	}

	path := s.SecretsPath(fmt.Sprintf("settings/%s/%s", databaseUuid, branchUuid))

	fileContents, err := os.ReadFile(path)

	if err != nil {
		return nil, err
	}

	decrypted, err := s.Decrypt(config.Get("signature"), string(fileContents))

	if err != nil {
		return nil, err
	}

	err = json.Unmarshal([]byte(decrypted["value"]), &value)

	if err != nil {
		return nil, err
	}

	s.cache("map").Put(s.databaseSettingCacheKey(databaseUuid, branchUuid), value, time.Second*60)
	s.cache("file").Put(s.databaseSettingCacheKey(databaseUuid, branchUuid), value, time.Second*60)

	return value.(map[string]interface{}), nil
}

func (s *SecretsManagerInstance) GetPath(databaseUuid string, branchUuid string) (string, error) {
	value := s.cache("transient").Get(fmt.Sprintf("%s:%s:path", databaseUuid, branchUuid))

	if value != nil {
		return value.(string), nil
	}

	value, err := s.GetDatabaseSettings(databaseUuid, branchUuid)

	if err != nil {
		return "", err
	}

	path, hasPath := value.(map[string]interface{})["path"]

	if hasPath {
		s.cache("transient").Put(fmt.Sprintf("%s:%s:path", databaseUuid, branchUuid), path, time.Second*1)

		return path.(string), nil
	}

	return "", nil
}

func (s *SecretsManagerInstance) GetSecret(accessKeyId string) (string, error) {
	value := s.cache("transient").Get(fmt.Sprintf("%s:secret", accessKeyId))

	if value != nil {
		return value.(string), nil
	}

	accessKey, err := s.GetAccessKey(accessKeyId)

	if err != nil {
		return "", err
	}

	s.cache("transient").Put(fmt.Sprintf("%s:secret", accessKeyId), accessKey.AccessKeySecret, time.Second*1)

	return accessKey.AccessKeySecret, nil
}

func (s *SecretsManagerInstance) GetServerSecret(accessKeyId string) (string, error) {
	value := s.cache("transient").Get(fmt.Sprintf("%s:server_secret", accessKeyId))

	if value != nil {
		return value.(string), nil
	}

	accessKey, err := s.GetAccessKey(accessKeyId)

	if err != nil {
		return "", err
	}

	s.cache("transient").Put(fmt.Sprintf("%s:server_secret", accessKeyId), accessKey.ServerAccessKeySecret, time.Second*1)

	return accessKey.ServerAccessKeySecret, nil
}

func (s *SecretsManagerInstance) Init() {
	// Check if the secrets path exists
	if _, err := os.Stat(s.SecretsPath("")); os.IsNotExist(err) {
		err := os.MkdirAll(s.SecretsPath(""), 0755)

		if err != nil {
			log.Fatal(err)
		}
	}

	if _, err := os.Stat(s.SecretsPath("access_keys")); os.IsNotExist(err) {
		err := os.MkdirAll(s.SecretsPath("access_keys"), 0755)

		if err != nil {
			log.Fatal(err)
		}
	}

	if _, err := os.Stat(s.SecretsPath("settings")); os.IsNotExist(err) {
		err := os.MkdirAll(s.SecretsPath("settings"), 0755)

		if err != nil {
			log.Fatal(err)
		}
	}
}

func (s *SecretsManagerInstance) PurgeAccessKey(accessKeyId string) {
	s.cache("map").Forget(s.accessKeyCacheKey(accessKeyId))
	s.cache("transient").Forget(s.accessKeyCacheKey(accessKeyId))
}

func (s *SecretsManagerInstance) PurgeAccessKeys() {
	// Get all the file names in the access keys directory
	files, err := os.ReadDir(s.SecretsPath("access_keys"))

	if err != nil {
		return
	}

	for _, file := range files {
		s.PurgeAccessKey(file.Name())
	}
}

func (s *SecretsManagerInstance) PurgeDatabaseSettings(databaseUuid string, branchUuid string) {
	s.cache("map").Forget(s.databaseSettingCacheKey(databaseUuid, branchUuid))
	s.cache("transient").Forget(s.databaseSettingCacheKey(databaseUuid, branchUuid))
	s.cache("file").Forget(s.databaseSettingCacheKey(databaseUuid, branchUuid))
}

func (s *SecretsManagerInstance) SecretsPath(key string) string {
	return fmt.Sprintf(
		"%s/.litebasedb/%s/%s",
		config.Get("data_path"),
		config.Get("signature"),
		key,
	)
}

func (s *SecretsManagerInstance) StoreAccessKey(
	databaseUuid string,
	branchUuid string,
	accessKeyId string,
	data string,
) {
	decryptedAccessKeyData, err := SecretsManager().Decrypt(config.Get("signature"), data)

	if err != nil {
		log.Fatal(err)
	}

	var jsonAccessKey map[string]interface{}

	err = json.Unmarshal([]byte(decryptedAccessKeyData["value"]), &jsonAccessKey)

	if err != nil {
		log.Fatal(err)
	}

	jsonValue, err := json.Marshal(jsonAccessKey)

	if err != nil {
		log.Fatal(err)
	}

	encryptedAccessKey, err := SecretsManager().Encrypt(
		config.Get("signature"),
		string(jsonValue),
	)

	if err != nil {
		log.Fatal(err)
	}

	os.WriteFile(
		s.SecretsPath(fmt.Sprintf("access_keys/%s", accessKeyId)),
		[]byte(encryptedAccessKey),
		0666,
	)
}

func (s *SecretsManagerInstance) StoreDatabaseSettings(
	databaseUuid string,
	branchUuid string,
	databaseKey string,
	data string,
) {
	if _, err := os.Stat(s.SecretsPath(fmt.Sprintf("settings/%s", databaseUuid))); os.IsNotExist(err) {
		os.MkdirAll(s.SecretsPath(fmt.Sprintf("settings/%s", databaseUuid)), 0755)
	}

	decryptedSettingsData, err := SecretsManager().Decrypt(config.Get("signature"), data)

	if err != nil {
		log.Fatal(err)
	}

	var jsonSettings map[string]interface{}

	err = json.Unmarshal([]byte(decryptedSettingsData["value"]), &jsonSettings)

	if err != nil {
		log.Fatal(err)
	}

	jsonValue, err := json.Marshal(jsonSettings)

	if err != nil {
		log.Fatal(err)
	}

	encryptedSettings, err := SecretsManager().Encrypt(
		config.Get("signature"),
		string(jsonValue),
	)

	if err != nil {
		log.Fatal(err)
	}

	os.WriteFile(
		s.SecretsPath(fmt.Sprintf("settings/%s/%s", databaseUuid, branchUuid)),
		[]byte(encryptedSettings),
		0666,
	)
}

func (s *SecretsManagerInstance) UpdateAccessKey(
	databaseUuid string,
	branchUuid string,
	accessKeyId string,
	privileges interface{},
) bool {
	accessKey, err := s.GetAccessKey(accessKeyId)

	if err != nil {
		return false
	}

	if accessKey == nil {
		return false
	}

	if databaseUuid != accessKey.GetDatabaseUuid() ||
		branchUuid != accessKey.GetBranchUuid() {
		return false
	}

	privilegesString, err := json.Marshal(privileges)

	if err != nil {
		return false
	}

	accessKeyPrivileges := AccessKeyPrivilegeGroups{}

	err = json.Unmarshal(privilegesString, &accessKeyPrivileges)

	if err != nil {
		return false
	}

	accessKey.Privileges = accessKeyPrivileges

	jsonValue, err := json.Marshal(accessKey)

	if err != nil {
		log.Fatal(err)
	}

	os.WriteFile(
		s.SecretsPath(fmt.Sprintf("access_keys/%s", accessKeyId)),
		jsonValue,
		0666,
	)

	return true
}
