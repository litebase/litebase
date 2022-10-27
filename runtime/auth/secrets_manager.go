package auth

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"litebasedb/runtime/config"
	"log"
	"os"
	"strings"
	"time"
)

type SecretsManagerData struct {
	secretStore map[string]SecretsStore
	encrypter   *Encrypter
}

var staticSecretsManager *SecretsManagerData

func SecretsManager() *SecretsManagerData {
	if staticSecretsManager == nil {
		key, err := base64.StdEncoding.DecodeString(strings.ReplaceAll(config.Get("encryption_key"), "base64:", ""))

		if err != nil {
			panic(err)
		}

		staticSecretsManager = &SecretsManagerData{
			encrypter:   NewEncrypter(key),
			secretStore: make(map[string]SecretsStore),
		}
	}

	return staticSecretsManager
}

func (s *SecretsManagerData) accessKeyCacheKey(accessKeyId string) string {
	return "access_key:" + accessKeyId
}

func (s *SecretsManagerData) cache(key string) SecretsStore {
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

func (s *SecretsManagerData) databaseSettingCacheKey(databaseUuid string, branchUuid string) string {
	return fmt.Sprintf("database_secret:%s:%s", databaseUuid, branchUuid)
}

func (s *SecretsManagerData) Decrypt(text string) (string, error) {
	return s.encrypter.Decrypt(text)
}

func (s *SecretsManagerData) DecryptFor(accessKeyId string, text string, secret string) (string, error) {
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

func (s *SecretsManagerData) DeleteAccessKey(accessKeyId string) {
	s.PurgeAccessKey(accessKeyId)

	path := s.SecretsPath(fmt.Sprintf("access_keys/%s.json", accessKeyId))

	os.Remove(path)
}

func (s *SecretsManagerData) DeleteSettings(databaseUuid string, branchUuid string) {
	path := s.SecretsPath(fmt.Sprintf("settings/%s", databaseUuid))
	os.RemoveAll(path)
}

func (s *SecretsManagerData) Encrypt(text string) (string, error) {
	return s.encrypter.Encrypt(text)
}

func (s *SecretsManagerData) EncryptFor(accessKeyId, text string) (string, error) {
	secret, err := s.GetSecret(accessKeyId)

	if err != nil {
		return "", err
	}

	encrypter := NewEncrypter([]byte(secret))

	return encrypter.Encrypt(text)
}

func (s *SecretsManagerData) FlushTransients() {
	s.cache("transient").Flush()
}

func (s *SecretsManagerData) GetAccessKey(accessKeyId string) (*AccessKey, error) {
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

	path := s.SecretsPath(fmt.Sprintf("access_keys/%s.json", accessKeyId))

	fileContents, err := os.ReadFile(path)

	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(fileContents, &accessKey)

	if err != nil {
		return nil, err
	}

	s.cache("map").Put(s.accessKeyCacheKey(accessKeyId), accessKey, time.Second*60)
	s.cache("file").Put(s.accessKeyCacheKey(accessKeyId), accessKey, time.Second*60)

	return accessKey, nil
}

func (s *SecretsManagerData) GetAwsCredentials(databaseUuid string, branchUuid string) (map[string]string, error) {
	value, err := s.GetDatabaseSettings(databaseUuid, branchUuid)

	if err != nil {
		return nil, err
	}

	if value == nil {
		return nil, nil
	}

	data, hasData := value["data"]

	if hasData {
		decrypted, err := s.Decrypt(data.(string))

		if err != nil {
			return nil, err
		}

		err = json.Unmarshal([]byte(decrypted), &value)

		if err != nil {
			return nil, err
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
	}

	return nil, nil
}

func (s *SecretsManagerData) GetBackupBucketName(databaseUuid string, branchUuid string) (string, error) {
	value, err := s.GetDatabaseSettings(databaseUuid, branchUuid)

	if err != nil {
		return "", err
	}

	if value == nil {
		return "", nil
	}

	data, hasData := value["data"]

	if hasData {
		decrypted, err := s.Decrypt(data.(string))

		if err != nil {
			return "", err
		}

		err = json.Unmarshal([]byte(decrypted), &value)

		if err != nil {
			return "", err
		}

		backupBucketName, hasBackupBucketName := value["backupBucketName"]

		if hasBackupBucketName {
			return backupBucketName.(string), nil
		}
	}

	return "", nil
}

func (s *SecretsManagerData) GetConnectionKey(databaseUuid string, branchUuid string) (string, error) {
	value, err := s.GetDatabaseSettings(databaseUuid, branchUuid)

	if err != nil {
		return "", err
	}

	if value == nil {
		return "", nil
	}

	data, hasData := value["data"]

	if hasData {
		decrypted, err := s.Decrypt(data.(string))

		if err != nil {
			return "", err
		}

		err = json.Unmarshal([]byte(decrypted), &value)

		if err != nil {
			return "", err
		}

		connectionKey, hasConnectionKey := value["connectionKey"]

		if hasConnectionKey {
			return connectionKey.(string), nil
		}
	}

	return "", nil
}

func (s *SecretsManagerData) GetDatabaseKey(accessKeyId string) (string, error) {
	accessKey, err := s.GetAccessKey(accessKeyId)

	if err != nil {
		return "", err
	}

	return accessKey.GetAccessKeyId(), nil

}

func (s *SecretsManagerData) GetDatabaseSettings(databaseUuid string, branchUuid string) (map[string]interface{}, error) {
	value := s.cache("map").Get(s.databaseSettingCacheKey(databaseUuid, branchUuid))

	if value != nil {
		return value.(map[string]interface{}), nil
	}

	value = s.cache("file").Get(s.databaseSettingCacheKey(databaseUuid, branchUuid))

	if value != nil {
		return value.(map[string]interface{}), nil
	}

	path := s.SecretsPath(fmt.Sprintf("settings/%s/%s.json", databaseUuid, branchUuid))
	fileContents, err := os.ReadFile(path)

	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(fileContents, &value)

	if err != nil {
		return nil, err
	}

	s.cache("map").Put(s.databaseSettingCacheKey(databaseUuid, branchUuid), value, time.Second*60)
	s.cache("file").Put(s.databaseSettingCacheKey(databaseUuid, branchUuid), value, time.Second*60)

	return value.(map[string]interface{}), nil
}

func (s *SecretsManagerData) GetPath(databaseUuid string, branchUuid string) (string, error) {
	value := s.cache("transient").Get(fmt.Sprintf("%s:%s:path", databaseUuid, branchUuid))

	if value != nil {
		return value.(string), nil
	}

	value, err := s.GetDatabaseSettings(databaseUuid, branchUuid)

	if err != nil {
		return "", err
	}

	data, hasData := value.(map[string]interface{})["data"]

	if hasData {
		decrypted, err := s.Decrypt(data.(string))

		if err != nil {
			return "", err
		}

		err = json.Unmarshal([]byte(decrypted), &value)

		if err != nil {
			return "", err
		}

		path, hasPath := value.(map[string]interface{})["path"]

		if hasPath {
			s.cache("transient").Put(fmt.Sprintf("%s:%s:path", databaseUuid, branchUuid), path, time.Second*1)

			return path.(string), nil
		}
	}

	return "", nil
}

func (s *SecretsManagerData) GetSecret(accessKeyId string) (string, error) {
	value := s.cache("transient").Get(fmt.Sprintf("%s:secret", accessKeyId))

	if value != nil {
		return value.(string), nil
	}

	accessKey, err := s.GetAccessKey(accessKeyId)

	if err != nil {
		return "", err
	}

	decrypted, err := s.Decrypt(accessKey.AccessKeySecret)

	if err != nil {
		return "", err
	}

	s.cache("transient").Put(fmt.Sprintf("%s:secret", accessKeyId), decrypted, time.Second*1)

	return decrypted, nil
}

func (s *SecretsManagerData) GetServerSecret(accessKeyId string) (string, error) {
	value := s.cache("transient").Get(fmt.Sprintf("%s:server_secret", accessKeyId))

	if value != nil {
		return value.(string), nil
	}

	accessKey, err := s.GetAccessKey(accessKeyId)

	if err != nil {
		return "", err
	}

	decrypted, err := s.Decrypt(accessKey.ServerAccessKeySecret)

	if err != nil {
		return "", err
	}

	s.cache("transient").Put(fmt.Sprintf("%s:server_secret", accessKeyId), decrypted, time.Second*1)

	return decrypted, nil
}

func (s *SecretsManagerData) Init() {
	// Check if the secrets path exists
	if _, err := os.Stat(s.SecretsPath("")); os.IsNotExist(err) {
		err := os.MkdirAll(s.SecretsPath(""), 0755)

		if err != nil {
			panic(err)
		}
	}

	if _, err := os.Stat(s.SecretsPath("access_keys")); os.IsNotExist(err) {
		err := os.MkdirAll(s.SecretsPath("access_keys"), 0755)

		if err != nil {
			panic(err)
		}
	}

	if _, err := os.Stat(s.SecretsPath("settings")); os.IsNotExist(err) {
		err := os.MkdirAll(s.SecretsPath("settings"), 0755)

		if err != nil {
			panic(err)
		}
	}
}

func (s *SecretsManagerData) PurgeAccessKey(accessKeyId string) {
	s.cache("map").Forget(s.accessKeyCacheKey(accessKeyId))
	s.cache("transient").Forget(s.accessKeyCacheKey(accessKeyId))
}

func (s *SecretsManagerData) PurgeAccessKeys() {
	// Get all the file names in the access keys directory
	files, err := os.ReadDir(s.SecretsPath("access_keys"))

	if err != nil {
		return
	}

	for _, file := range files {
		s.PurgeAccessKey(strings.ReplaceAll(file.Name(), ".json", ""))
	}
}

func (s *SecretsManagerData) PurgeDatabaseSettings(databaseUuid string, branchUuid string) {
	s.cache("map").Forget(s.databaseSettingCacheKey(databaseUuid, branchUuid))
	s.cache("transient").Forget(s.databaseSettingCacheKey(databaseUuid, branchUuid))
	s.cache("file").Forget(s.databaseSettingCacheKey(databaseUuid, branchUuid))
}

func (s *SecretsManagerData) SecretsPath(key string) string {
	return fmt.Sprintf(
		"%s/.litebasedb/%s",
		strings.TrimRight(config.Get("data_path"), "/"),
		strings.TrimRight(key, "/"),
	)
}

func (s *SecretsManagerData) StoreAccessKey(
	databaseUuid string,
	branchUuid string,
	accessKeyId string,
	accessKeySecret string,
	serverAccessKeySecret string,
	privileges map[string]interface{},
) {
	jsonValue, err := json.Marshal(map[string]interface{}{
		"database_uuid":            databaseUuid,
		"branch_uuid":              branchUuid,
		"access_key_id":            accessKeyId,
		"access_key_secret":        accessKeySecret,
		"server_access_key_secret": serverAccessKeySecret,
		"privileges":               privileges,
	})

	if err != nil {
		panic(err)
	}

	os.WriteFile(
		s.SecretsPath(fmt.Sprintf("access_keys/%s.json", accessKeyId)),
		jsonValue,
		0644,
	)
}

func (s *SecretsManagerData) StoreDatabaseSettings(
	databaseUuid string,
	branchUuid string,
	databaseKey string,
	branchSettings map[string]interface{},
	data string,
) {
	if _, err := os.Stat(s.SecretsPath(fmt.Sprintf("settings/%s", databaseUuid))); os.IsNotExist(err) {
		os.MkdirAll(s.SecretsPath(fmt.Sprintf("settings/%s", databaseUuid)), 0755)
	}

	jsonValue, err := json.Marshal(map[string]interface{}{
		"database_uuid":   databaseUuid,
		"branch_uuid":     branchUuid,
		"database_key":    databaseKey,
		"branch_settings": branchSettings,
		"data":            data,
	})

	if err != nil {
		panic(err)
	}

	os.WriteFile(
		s.SecretsPath(fmt.Sprintf("settings/%s/%s.json", databaseUuid, branchUuid)),
		jsonValue,
		0644,
	)
}

func (s *SecretsManagerData) UpdateAccessKey(
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

	log.Println(accessKeyPrivileges)

	accessKey.Privileges = accessKeyPrivileges

	jsonValue, err := json.Marshal(accessKey)

	if err != nil {
		panic(err)
	}

	os.WriteFile(
		s.SecretsPath(fmt.Sprintf("access_keys/%s.json", accessKeyId)),
		jsonValue,
		0644,
	)

	return true
}
