package auth

import (
	"encoding/json"
	"fmt"
	_auth "litebasedb/internal/auth"
	"litebasedb/internal/config"
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

func (s *SecretsManagerInstance) accessKeyCacheKey(signatureHash, accessKeyId string) string {
	return fmt.Sprintf("access_key:%s:%s", signatureHash, accessKeyId)
}

func (s *SecretsManagerInstance) accessKeySecretCacheKey(signatureHash, accessKeyId string) string {
	return fmt.Sprintf("access_key_secret:%s:%s", signatureHash, accessKeyId)
}

func (s *SecretsManagerInstance) accessKeyServerSecretCacheKey(signatureHash, accessKeyId string) string {
	return fmt.Sprintf("access_key_server_secret:%s:%s", signatureHash, accessKeyId)
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

func (s *SecretsManagerInstance) databaseSettingsCacheKey(signatureHash, databaseUuid, branchUuid string) string {
	return fmt.Sprintf("database_settings:%s:%s:%s", signatureHash, databaseUuid, branchUuid)
}

func (s *SecretsManagerInstance) Decrypt(signature string, text string) (map[string]string, error) {
	return s.Encrypter(signature).Decrypt(text)
}

func (s *SecretsManagerInstance) DecryptFor(signatureHash, accessKeyId, text, secret string) (string, error) {
	var err error

	if secret == "" {
		secret, err = s.GetSecret(signatureHash, accessKeyId)

		if err != nil {
			return "", err
		}
	}

	encrypter := NewEncrypter([]byte(secret))

	return encrypter.Decrypt(text)
}

func (s *SecretsManagerInstance) DeleteAccessKey(accessKeyId string) {
	s.PurgeAccessKey(accessKeyId)

	for _, signature := range _auth.AllSignatures() {
		path := s.SecretsPath(signature, fmt.Sprintf("access_keys/%s", accessKeyId))

		os.Remove(path)
	}

}

func (s *SecretsManagerInstance) DeleteSettings(databaseUuid string, branchUuid string) {
	for _, signature := range _auth.AllSignatures() {
		path := s.SecretsPath(signature, fmt.Sprintf("settings/%s", databaseUuid))
		os.RemoveAll(path)
	}
}

func (s *SecretsManagerInstance) Encrypt(signature string, text string) (string, error) {
	return s.Encrypter(signature).Encrypt(text)
}

func (s *SecretsManagerInstance) Encrypter(signature string) *KeyEncrypter {
	s.mutex.Lock()

	if _, ok := s.encrypterInstances[signature]; !ok {
		s.encrypterInstances[signature] = NewKeyEncrypter(signature)
	}

	s.mutex.Unlock()

	return s.encrypterInstances[signature]
}

func (s *SecretsManagerInstance) EncryptFor(signatureHash, accessKeyId, text string) (string, error) {
	secret, err := s.GetSecret(signatureHash, accessKeyId)

	if err != nil {
		return "", err
	}

	encrypter := NewEncrypter([]byte(secret))

	return encrypter.Encrypt(text)
}

func (s *SecretsManagerInstance) FlushTransients() {
	s.cache("transient").Flush()
}

func (s *SecretsManagerInstance) GetAccessKey(signatureHash, accessKeyId string) (*AccessKey, error) {
	var accessKey *AccessKey
	value := s.cache("map").Get(s.accessKeyCacheKey(signatureHash, accessKeyId))

	if value != nil {
		accessKey, ok := value.(*AccessKey)

		if ok {
			return accessKey, nil
		}
	}

	fileValue := s.cache("file").Get(s.accessKeyCacheKey(signatureHash, accessKeyId))

	if fileValue != nil {
		json.Unmarshal([]byte(fileValue.(string)), &accessKey)
	}

	if accessKey != nil {
		return accessKey, nil
	}

	var signature string

	if signature = _auth.FindSignature(signatureHash); signature == "" {
		return nil, fmt.Errorf("signature not found")
	}

	path := s.SecretsPath(signature, fmt.Sprintf("access_keys/%s", accessKeyId))

	fileContents, err := os.ReadFile(path)

	if err != nil {
		return nil, err
	}

	decrypted, err := s.Decrypt(signature, string(fileContents))

	if err != nil {
		return nil, err
	}

	err = json.Unmarshal([]byte(decrypted["value"]), &accessKey)

	if err != nil {
		return nil, err
	}

	s.cache("map").Put(s.accessKeyCacheKey(signatureHash, accessKeyId), accessKey, time.Second*60)
	s.cache("file").Put(s.accessKeyCacheKey(signatureHash, accessKeyId), accessKey, time.Second*60)

	return accessKey, nil
}

func (s *SecretsManagerInstance) GetAwsCredentials(databaseUuid, branchUuid string) (map[string]string, error) {
	value, err := s.GetDatabaseSettings("", databaseUuid, branchUuid)

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

func (s *SecretsManagerInstance) GetBackupBucketName(signature, databaseUuid string, branchUuid string) (string, error) {
	value, err := s.GetDatabaseSettings(signature, databaseUuid, branchUuid)

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

func (s *SecretsManagerInstance) GetConnectionKey(signatureHash, databaseUuid string, branchUuid string) (string, error) {
	value, err := s.GetDatabaseSettings(signatureHash, databaseUuid, branchUuid)

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

func (s *SecretsManagerInstance) GetDatabaseKey(signatureHash, accessKeyId string) (string, error) {
	accessKey, err := s.GetAccessKey(signatureHash, accessKeyId)

	if err != nil {
		return "", err
	}

	return accessKey.GetAccessKeyId(), nil

}

func (s *SecretsManagerInstance) GetDatabaseSettings(signatureHash, databaseUuid, branchUuid string) (map[string]interface{}, error) {
	if signatureHash == "" {
		signatureHash = _auth.ActiveSignatureHash()
	}

	value := s.cache("map").Get(s.databaseSettingsCacheKey(signatureHash, databaseUuid, branchUuid))

	if value != nil {
		return value.(map[string]interface{}), nil
	}

	signature := _auth.FindSignature(signatureHash)

	if signature == "" {
		return nil, fmt.Errorf("signature not found")
	}

	value = s.cache("file").Get(s.databaseSettingsCacheKey(signatureHash, databaseUuid, branchUuid))

	if value != nil {
		return value.(map[string]interface{}), nil
	}

	path := s.SecretsPath(signature, fmt.Sprintf("settings/%s/%s", databaseUuid, branchUuid))

	fileContents, err := os.ReadFile(path)

	if err != nil {
		return nil, err
	}

	decrypted, err := s.Decrypt(signature, string(fileContents))

	if err != nil {
		return nil, err
	}

	err = json.Unmarshal([]byte(decrypted["value"]), &value)

	if err != nil {
		return nil, err
	}

	s.cache("map").Put(s.databaseSettingsCacheKey(signatureHash, databaseUuid, branchUuid), value, time.Second*60)
	s.cache("file").Put(s.databaseSettingsCacheKey(signatureHash, databaseUuid, branchUuid), value, time.Second*60)

	return value.(map[string]interface{}), nil
}

func (s *SecretsManagerInstance) GetPath(databaseUuid string, branchUuid string) (string, error) {
	value := s.cache("transient").Get(fmt.Sprintf("%s:%s:path", databaseUuid, branchUuid))

	if value != nil {
		return value.(string), nil
	}

	value, err := s.GetDatabaseSettings("", databaseUuid, branchUuid)

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

func (s *SecretsManagerInstance) GetSecret(signatureHash, accessKeyId string) (string, error) {
	value := s.cache("transient").Get(s.accessKeySecretCacheKey(signatureHash, accessKeyId))

	if value != nil {
		return value.(string), nil
	}

	accessKey, err := s.GetAccessKey(signatureHash, accessKeyId)

	if err != nil {
		return "", err
	}

	s.cache("transient").Put(s.accessKeySecretCacheKey(signatureHash, accessKeyId), accessKey.AccessKeySecret, time.Second*1)

	return accessKey.AccessKeySecret, nil
}

func (s *SecretsManagerInstance) GetServerSecret(signatureHash, accessKeyId string) (string, error) {
	value := s.cache("transient").Get(s.accessKeyServerSecretCacheKey(signatureHash, accessKeyId))

	if value != nil {
		return value.(string), nil
	}

	accessKey, err := s.GetAccessKey(signatureHash, accessKeyId)

	if err != nil {
		return "", err
	}

	s.cache("transient").Put(s.accessKeyServerSecretCacheKey(signatureHash, accessKeyId), accessKey.ServerAccessKeySecret, time.Second*1)

	return accessKey.ServerAccessKeySecret, nil
}

func (s *SecretsManagerInstance) Init() {
	// Check if the secrets path exists
	if _, err := os.Stat(s.SecretsPath(config.Get("signature"), "")); os.IsNotExist(err) {
		err := os.MkdirAll(s.SecretsPath(config.Get("signature"), ""), 0755)

		if err != nil {
			log.Fatal(err)
		}
	}

	if _, err := os.Stat(s.SecretsPath(config.Get("signature"), "access_keys")); os.IsNotExist(err) {
		err := os.MkdirAll(s.SecretsPath(config.Get("signature"), "access_keys"), 0755)

		if err != nil {
			log.Fatal(err)
		}
	}

	if _, err := os.Stat(s.SecretsPath(config.Get("signature"), "settings")); os.IsNotExist(err) {
		err := os.MkdirAll(s.SecretsPath(config.Get("signature"), "settings"), 0755)

		if err != nil {
			log.Fatal(err)
		}
	}

	s.PurgeExpiredSecrets()
}

func (s *SecretsManagerInstance) PurgeAccessKey(accessKeyId string) {
	for signatureHash := range _auth.AllSignatures() {
		s.cache("map").Forget(s.accessKeyCacheKey(signatureHash, accessKeyId))
		s.cache("transient").Forget(s.accessKeyCacheKey(signatureHash, accessKeyId))
	}
}

func (s *SecretsManagerInstance) PurgeAccessKeys() {
	for _, signature := range _auth.AllSignatures() {

		// Get all the file names in the access keys directory
		files, err := os.ReadDir(s.SecretsPath(signature, "access_keys"))

		if err != nil {
			return
		}

		for _, file := range files {
			s.PurgeAccessKey(file.Name())
		}
	}
}

func (s *SecretsManagerInstance) PurgeDatabaseSettings(databaseUuid string, branchUuid string) {
	for signatureHash := range _auth.AllSignatures() {
		s.cache("map").Forget(s.databaseSettingsCacheKey(signatureHash, databaseUuid, branchUuid))
		s.cache("transient").Forget(s.databaseSettingsCacheKey(signatureHash, databaseUuid, branchUuid))
		s.cache("file").Forget(s.databaseSettingsCacheKey(signatureHash, databaseUuid, branchUuid))
	}
}

func (s *SecretsManagerInstance) PurgeExpiredSecrets() {
	// Get all the file names in the litebasedb directory
	directories, err := os.ReadDir(fmt.Sprintf("%s/.litebasedb", config.Get("data_path")))

	if err != nil {
		log.Println(err)

		return
	}

	if len(directories) <= 2 {
		return
	}

	for _, directory := range directories {
		// Check if there is a manifest file
		manifestPath := fmt.Sprintf("%s/.litebasedb/%s/manifest.json", config.Get("data_path"), directory.Name())

		if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
			continue
		}

		// Check if the signature is still valid
		manifest, err := os.ReadFile(manifestPath)

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
			err := os.RemoveAll(fmt.Sprintf("%s/.litebasedb/%s", config.Get("data_path"), directory.Name()))

			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

func (s *SecretsManagerInstance) SecretsPath(signature, key string) string {
	return fmt.Sprintf(
		"%s/.litebasedb/%s/%s",
		config.Get("data_path"),
		signature,
		key,
	)
}

func (s *SecretsManagerInstance) StoreAccessKey(
	databaseUuid string,
	branchUuid string,
	accessKeyId string,
	data string,
) {
	// TODO: If the current runtime is not in sync with the signature that was used to
	// encrypt the data, this will fail.
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
		s.SecretsPath(config.Get("signature"), fmt.Sprintf("access_keys/%s", accessKeyId)),
		[]byte(encryptedAccessKey),
		0666,
	)
}

func (s *SecretsManagerInstance) StoreDatabaseSettings(
	databaseUuid string,
	branchUuid string,
	databaseKey string,
	data string,
) error {
	// TODO: Need to check if the current runtime is in sync with the signature that was used to
	// encrypt the data.
	if _, err := os.Stat(s.SecretsPath(config.Get("signature"), fmt.Sprintf("settings/%s", databaseUuid))); os.IsNotExist(err) {
		os.MkdirAll(s.SecretsPath(config.Get("signature"), fmt.Sprintf("settings/%s", databaseUuid)), 0755)
	}

	decryptedSettingsData, err := SecretsManager().Decrypt(config.Get("signature"), data)

	if err != nil {
		return err
	}

	var jsonSettings map[string]interface{}

	err = json.Unmarshal([]byte(decryptedSettingsData["value"]), &jsonSettings)

	if err != nil {
		return err
	}

	jsonValue, err := json.Marshal(jsonSettings)

	if err != nil {
		return err
	}

	encryptedSettings, err := SecretsManager().Encrypt(
		config.Get("signature"),
		string(jsonValue),
	)

	if err != nil {
		return err
	}

	os.WriteFile(
		s.SecretsPath(config.Get("signature"), fmt.Sprintf("settings/%s/%s", databaseUuid, branchUuid)),
		[]byte(encryptedSettings),
		0666,
	)

	return nil
}

func (s *SecretsManagerInstance) UpdateAccessKey(
	databaseUuid,
	branchUuid,
	accessKeyId string,
	privileges interface{},
) bool {
	for signatureHash, signature := range _auth.AllSignatures() {
		accessKey, err := s.GetAccessKey(signatureHash, accessKeyId)

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
			s.SecretsPath(signature, fmt.Sprintf("access_keys/%s", accessKeyId)),
			jsonValue,
			0666,
		)
	}

	return true
}
