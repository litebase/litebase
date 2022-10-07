package secrets

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"litebasedb/runtime/app/config"
	"os"
	"strings"
	"time"
)

type SecretsManagerStuct struct {
	secretStore map[string]SecretsStore
	encrypter   Encrypter
}

var staticSecretsManager *SecretsManagerStuct

func Manager() *SecretsManagerStuct {
	if staticSecretsManager != nil {
		return staticSecretsManager
	}

	staticSecretsManager = &SecretsManagerStuct{
		encrypter:   Encrypter{},
		secretStore: make(map[string]SecretsStore),
	}

	return staticSecretsManager
}

func (s *SecretsManagerStuct) accessKeyCacheKey(accessKeyId string) string {
	return "access_key:" + accessKeyId
}

func (s *SecretsManagerStuct) cache(key string) SecretsStore {
	_, hasFileStore := s.secretStore["file"]
	_, hasMapStore := s.secretStore["map"]
	_, hasTransientStore := s.secretStore["transient"]

	if key == "map" && !hasMapStore {
		s.secretStore["map"] = NewMapSecretsStore()
	}

	if key == "transient" && !hasTransientStore {
		s.secretStore["map"] = NewMapSecretsStore()
	}

	if key == "file" && !hasFileStore {
		s.secretStore["map"] = NewFileSecretsStore(
			config.Get("tmp_path") + "/litebasedb/cache",
		)
	}

	return s.secretStore[key]
}

func (s *SecretsManagerStuct) databaseSettingCacheKey(databaseUuid string, branchUuid string) string {
	return fmt.Sprintf("database_secret:%s:%s", databaseUuid, branchUuid)
}

func (s *SecretsManagerStuct) Decrypt(text string) (string, error) {
	return s.encrypter.Decrypt(text)
}

func (s *SecretsManagerStuct) DecryptFor(accessKeyId string, text string, secret string) (string, error) {
	if secret == "" {
		secret = s.GetSecret(accessKeyId)
	}

	// MD5 hash the secret
	hash := md5.New()
	hash.Write([]byte(secret))
	secret = fmt.Sprintf("%x", hash.Sum(nil))

	encrypter := NewEncrypter([]byte(secret))

	return encrypter.Decrypt(text)
}

func (s *SecretsManagerStuct) DeleteAccessKey(accessKeyId string) {
	s.PurgeAccessKey(accessKeyId)

	path := s.SecretsPath(fmt.Sprintf("access_keys/%s.json", accessKeyId))

	os.Remove(path)
}

func (s *SecretsManagerStuct) DeleteSettings(databaseUuid string, branchUuid string) {
	path := s.SecretsPath(fmt.Sprintf("settings/%s.json", databaseUuid))
	os.Remove(path)
}

func (s *SecretsManagerStuct) Encrypt(text string) (string, error) {
	return s.encrypter.Encrypt(text)
}

func (s *SecretsManagerStuct) EncryptFor(accessKeyId, text string) (string, error) {
	secret := s.GetSecret(accessKeyId)
	encrypter := NewEncrypter([]byte(secret))

	return encrypter.Encrypt(text)
}

func (s *SecretsManagerStuct) FlushTransients() {
	s.cache("transient").Flush()
}

func (s *SecretsManagerStuct) GetAccessKey(accessKeyId string) interface{} {
	value := s.cache("map").Get(s.accessKeyCacheKey(accessKeyId))

	if value != nil {
		return value
	}

	value = s.cache("file").Get(s.accessKeyCacheKey(accessKeyId))

	if value != nil {
		return value.(map[string]string)
	}

	path := s.SecretsPath(fmt.Sprintf("access_keys/%s.json", accessKeyId))

	fileContents, err := os.ReadFile(path)

	if err != nil {
		return nil
	}

	err = json.Unmarshal(fileContents, &value)

	if err != nil {
		return nil
	}

	s.cache("map").Put(s.accessKeyCacheKey(accessKeyId), value.(string), time.Second*60)
	s.cache("file").Put(s.accessKeyCacheKey(accessKeyId), value.(string), time.Second*60)

	return value
}

func (s *SecretsManagerStuct) GetAwsCredentials(databaseUuid string, branchUuid string) (map[string]string, error) {
	value := s.GetDatabaseSettings(databaseUuid, branchUuid)

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

func (s *SecretsManagerStuct) GetBackupBucketName(databaseUuid string, branchUuid string) string {
	value := s.GetDatabaseSettings(databaseUuid, branchUuid)

	if value == nil {
		return ""
	}

	data, hasData := value["data"]

	if hasData {
		decrypted, err := s.Decrypt(data.(string))

		if err != nil {
			return ""
		}

		err = json.Unmarshal([]byte(decrypted), &value)

		if err != nil {
			return ""
		}

		backupBucketName, hasBackupBucketName := value["backupBucketName"]

		if hasBackupBucketName {
			return backupBucketName.(string)
		}
	}

	return ""
}

func (s *SecretsManagerStuct) GetConnectionKey(databaseUuid string, branchUuid string) string {
	value := s.GetDatabaseSettings(databaseUuid, branchUuid)

	if value == nil {
		return ""
	}

	data, hasData := value["data"]

	if hasData {
		decrypted, err := s.Decrypt(data.(string))

		if err != nil {
			return ""
		}

		err = json.Unmarshal([]byte(decrypted), &value)

		if err != nil {
			return ""
		}

		connectionKey, hasConnectionKey := value["connectionKey"]

		if hasConnectionKey {
			return connectionKey.(string)
		}
	}

	return ""
}

func (s *SecretsManagerStuct) GetDatabaseKey(accessKeyId string) string {
	value := s.GetAccessKey(accessKeyId)

	if value == nil {
		return ""
	}

	databaseKey, hasDatabaseKey := value.(map[string]string)["databaseKey"]

	if hasDatabaseKey {
		return databaseKey
	}

	return ""
}

func (s *SecretsManagerStuct) GetDatabaseSettings(databaseUuid string, branchUuid string) map[string]interface{} {
	value := s.cache("map").Get(s.databaseSettingCacheKey(databaseUuid, branchUuid))

	if value != nil {
		return value.(map[string]interface{})
	}

	value = s.cache("file").Get(s.databaseSettingCacheKey(databaseUuid, branchUuid))

	if value != nil {
		return value.(map[string]interface{})
	}

	path := s.SecretsPath(fmt.Sprintf("settings/%s/%s.json", databaseUuid, branchUuid))

	fileContents, err := os.ReadFile(path)

	if err != nil {
		return nil
	}

	err = json.Unmarshal(fileContents, &value)

	if err != nil {
		return nil
	}

	s.cache("map").Put(s.databaseSettingCacheKey(databaseUuid, branchUuid), value.(string), time.Second*60)
	s.cache("file").Put(s.databaseSettingCacheKey(databaseUuid, branchUuid), value.(string), time.Second*60)

	return value.(map[string]interface{})
}

func (s *SecretsManagerStuct) GetPath(databaseUuid string, branchUuid string) string {
	value := s.cache("transient").Get(fmt.Sprintf("%s:%s:path", databaseUuid, branchUuid))

	if value != nil {
		return value.(string)
	}

	value = s.GetDatabaseSettings(databaseUuid, branchUuid)

	if value == nil {
		return ""
	}

	data, hasData := value.(map[string]string)["data"]

	if hasData {
		decrypted, err := s.Decrypt(data)

		if err != nil {
			return ""
		}

		err = json.Unmarshal([]byte(decrypted), &value)

		if err != nil {
			return ""
		}

		path, hasPath := value.(map[string]string)["path"]

		if hasPath {
			s.cache("transient").Put(fmt.Sprintf("%s:%s:path", databaseUuid, branchUuid), path, time.Second*1)

			return path
		}
	}

	return ""
}

func (s *SecretsManagerStuct) GetSecret(accessKeyId string) string {
	value := s.cache("transient").Get(fmt.Sprintf("%s:secret", accessKeyId))

	if value != nil {
		return value.(string)
	}

	value = s.GetAccessKey(accessKeyId)
	decrypted, err := s.Decrypt(value.(map[string]string)["access_key_secret"])

	if err != nil {
		return ""
	}

	s.cache("transient").Put(fmt.Sprintf("%s:secret", accessKeyId), decrypted, time.Second*1)

	return decrypted
}

func (s *SecretsManagerStuct) GetServerSecret(accessKeyId string) string {
	value := s.cache("transient").Get(fmt.Sprintf("%s:server_secret", accessKeyId))

	if value != nil {
		return value.(string)
	}

	value = s.GetAccessKey(accessKeyId)
	decrypted, err := s.Decrypt(value.(map[string]string)["server_access_key_secret"])

	if err != nil {
		return ""
	}

	s.cache("transient").Put(fmt.Sprintf("%s:server_secret", accessKeyId), decrypted, time.Second*1)

	return decrypted
}

func (s *SecretsManagerStuct) Init() {
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

func (s *SecretsManagerStuct) PurgeAccessKey(accessKeyId string) {
	s.cache("map").Forget(s.accessKeyCacheKey(accessKeyId))
	s.cache("transient").Forget(s.accessKeyCacheKey(accessKeyId))
}

func (s *SecretsManagerStuct) PurgeAccessKeys() {
	// Get all the file names in the access keys directory
	files, err := os.ReadDir(s.SecretsPath("access_keys"))

	if err != nil {
		return
	}

	for _, file := range files {
		s.PurgeAccessKey(strings.ReplaceAll(file.Name(), ".json", ""))
	}
}

func (s *SecretsManagerStuct) PurgeDatabaseSetting(databaseUuid string, branchUuid string) {
	s.cache("map").Forget(s.databaseSettingCacheKey(databaseUuid, branchUuid))
	s.cache("file").Forget(s.databaseSettingCacheKey(databaseUuid, branchUuid))
}

func (s *SecretsManagerStuct) SecretsPath(key string) string {
	return fmt.Sprintf(
		"%s/.litebasedb/%s",
		strings.TrimRight(os.Getenv("LITEBASEDB_DATA_PATH"), "/"),
		strings.TrimRight(key, "/"),
	)
}

func (s *SecretsManagerStuct) StoreAccessKey(
	databaseUuid string,
	branchUuid string,
	accessKeyId string,
	accessKeySecret string,
	serverAccessKeySecret string,
	privileges map[string][]string,
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

func (s *SecretsManagerStuct) StoreDatabaseSetting(
	databaseUuid string,
	branchUuid string,
	databaseKey string,
	branchSettings map[string]interface{},
	data map[string]string,
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

func (s *SecretsManagerStuct) UpdateAccessKey(
	databaseUuid string,
	branchUuid string,
	accessKeyId string,
	privileges map[string][]string,
) bool {
	accessKey := s.GetAccessKey(accessKeyId)

	if accessKey == nil {
		return false
	}

	if databaseUuid != accessKey.(map[string]interface{})["database_uuid"] ||
		branchUuid != accessKey.(map[string]interface{})["branch_uuid"] {
		return false
	}

	accessKey.(map[string]interface{})["privileges"] = privileges

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
