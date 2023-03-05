package auth

import (
	"encoding/json"
	"fmt"
	_auth "litebasedb/internal/auth"
	"litebasedb/internal/config"
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
	encrypterInstances map[string]*KeyEncrypter
	mutex              *sync.Mutex
}

var staticSecretsManager *SecretsManagerInstance

func SecretsManager() *SecretsManagerInstance {
	if staticSecretsManager == nil {
		staticSecretsManager = &SecretsManagerInstance{
			databaseKeys:       make(map[string]string),
			encrypterInstances: make(map[string]*KeyEncrypter),
			mutex:              &sync.Mutex{},
			secretStore:        make(map[string]SecretsStore),
		}
	}

	return staticSecretsManager
}

func (s *SecretsManagerInstance) accessKeyCacheKey(accessKeyId string) string {
	return fmt.Sprintf("access_key:%s", accessKeyId)
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

func (s *SecretsManagerInstance) databaseSettingCacheKey(databaseUuid, branchUuid string) string {
	return fmt.Sprintf("database_secret:%s:%s", databaseUuid, branchUuid)
}

func (s *SecretsManagerInstance) Decrypt(signature string, text string) (map[string]string, error) {
	return s.Encrypter(signature).Decrypt(text)
}

func (s *SecretsManagerInstance) DeleteAccessKey(accessKeyId string) {
	signatures := _auth.AllSignatures()

	for _, signature := range signatures {
		s.PurgeAccessKey(accessKeyId)

		path := s.SecretsPath(signature, fmt.Sprintf("access_keys/%s", accessKeyId))

		os.Remove(path)
	}
}

func (s *SecretsManagerInstance) DeleteDatabaseKey(databaseKey string) {
	format := "%s/%s/%s"

	filePaths := []string{
		fmt.Sprintf(format, Path(config.Get("signature")), "database_keys", databaseKey),
	}

	if config.Get("signature_next") != "" {
		filePaths = append(filePaths, fmt.Sprintf(format, Path(config.Get("signature_next")), "database_keys", databaseKey))
	}

	for _, filePath := range filePaths {
		err := os.Remove(filePath)

		if err != nil {
			log.Println(err)
		}
	}
}

func (s *SecretsManagerInstance) DeleteSettings(databaseUuid string, branchUuid string) {
	signatures := _auth.AllSignatures()

	for _, signature := range signatures {
		path := s.SecretsPath(signature, fmt.Sprintf("settings/%s", databaseUuid))
		os.RemoveAll(path)
	}
}

func (s *SecretsManagerInstance) Encrypt(signature string, text string) (string, error) {
	return s.Encrypter(EncrypterKey(signature, "")).Encrypt(text)
}

func (s *SecretsManagerInstance) EncryptForRuntime(databaseUuid, signature, text string) (string, error) {
	return s.Encrypter(EncrypterKey(signature, databaseUuid)).Encrypt(text)
}

func (s *SecretsManagerInstance) Encrypter(key string) *KeyEncrypter {
	s.mutex.Lock()

	if _, ok := s.encrypterInstances[key]; !ok {
		params := strings.Split(key, ":")
		signature := params[0]

		if len(params) > 1 {
			s.encrypterInstances[key] = NewKeyEncrypter(signature).ForDatabase(params[1])
		} else {
			s.encrypterInstances[key] = NewKeyEncrypter(signature)
		}
	}

	s.mutex.Unlock()

	return s.encrypterInstances[key]
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

func (s *SecretsManagerInstance) GetAccessKey(databaseUuid, accessKeyId string) (*AccessKey, error) {
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

	path := s.SecretsPath(config.Get("signature"), fmt.Sprintf("access_keys/%s", accessKeyId))

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

func (s *SecretsManagerInstance) GetAwsCredentials(databaseUuid string, branchUuid string) (*AWSCredentials, error) {
	value, err := s.GetDatabaseSettings(databaseUuid, branchUuid)

	if err != nil {
		return nil, err
	}

	if value == nil {
		return nil, nil
	}

	awsAccessToken, hasAwsAccessToken := value["awsAccessToken"]

	if hasAwsAccessToken {
		credentials := strings.Split(awsAccessToken.(string), ":")

		return &AWSCredentials{
			Key:    credentials[0],
			Secret: credentials[1],
			Token:  credentials[1],
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

func (s *SecretsManagerInstance) GetDatabaseKey(databaseUuid, accessKeyId string) (string, error) {
	settings, err := s.GetDatabaseSettings(databaseUuid, accessKeyId)

	if err != nil {
		return "", err
	}

	return settings["databaseKey"].(string), nil
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

	path := s.SecretsPath(
		config.Get("signature"),
		fmt.Sprintf("settings/%s/%s", databaseUuid, branchUuid),
	)

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

func (s *SecretsManagerInstance) GetDatabaseUuid(databaseKey string) string {
	uuid := s.databaseKeys[databaseKey]

	if uuid != "" {
		return uuid
	}

	filePath := s.SecretsPath(
		config.Get("signature"),
		fmt.Sprintf("database_keys/%s", databaseKey),
	)

	databaseUuid, err := os.ReadFile(filePath)

	if err != nil {
		return ""
	}

	return string(databaseUuid)
}

func (s *SecretsManagerInstance) GetFunctionName(databaseUuid string, branchUuid string) (string, error) {
	value, err := s.GetDatabaseSettings(databaseUuid, branchUuid)

	if err != nil {
		return "", err
	}

	if value == nil {
		return "", nil
	}

	functionName, hasFunctionName := value["function"]

	if hasFunctionName {
		return functionName.(string), nil
	}

	return "", nil
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

func (s *SecretsManagerInstance) GetPublicKey(signature, databaseUuid string) (string, error) {
	value := s.cache("map").Get(s.publicKeyCacheKey(signature, databaseUuid))

	if value != nil {
		publicKey, ok := value.(string)

		if ok {
			return publicKey, nil
		}
	}

	fileValue := s.cache("file").Get(s.publicKeyCacheKey(signature, databaseUuid))

	if fileValue != nil {
		publicKey, ok := fileValue.(string)

		if ok {
			return publicKey, nil
		}
	}

	path := s.SecretsPath(
		config.Get("signature"),
		fmt.Sprintf("public_keys/%s/public_key", databaseUuid),
	)

	key, err := os.ReadFile(path)

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

func (s *SecretsManagerInstance) GetServerSecret(databaseUuid, accessKeyId string) (string, error) {
	value := s.cache("transient").Get(fmt.Sprintf("%s:server_secret", accessKeyId))

	if value != nil {
		return value.(string), nil
	}

	accessKey, err := s.GetAccessKey(databaseUuid, accessKeyId)

	if err != nil {
		return "", err
	}

	s.cache("transient").Put(fmt.Sprintf("%s:server_secret", accessKeyId), accessKey.ServerAccessKeySecret, time.Second*1)

	return accessKey.ServerAccessKeySecret, nil
}

func (s *SecretsManagerInstance) HasAccessKey(databaseKey, accessKeyId string) bool {
	databaseUuid := SecretsManager().GetDatabaseUuid(databaseKey)

	_, err := s.GetAccessKey(databaseUuid, accessKeyId)

	return err == nil
}

func (s *SecretsManagerInstance) Init() {
	// Ensure the secrets path exists
	if _, err := os.Stat(s.SecretsPath(config.Get("signature"), "")); os.IsNotExist(err) {
		err := os.MkdirAll(s.SecretsPath(config.Get("signature"), ""), 0755)

		if err != nil {
			log.Fatal(err)
		}
	}

	// Ensure the access keys path exists
	if _, err := os.Stat(s.SecretsPath(config.Get("signature"), "access_keys")); os.IsNotExist(err) {
		err := os.MkdirAll(s.SecretsPath(config.Get("signature"), "access_keys"), 0755)

		if err != nil {
			log.Fatal(err)
		}
	}

	// Ensure the settings path exists
	if _, err := os.Stat(s.SecretsPath(config.Get("signature"), "settings")); os.IsNotExist(err) {
		err := os.MkdirAll(s.SecretsPath(config.Get("signature"), "settings"), 0755)

		if err != nil {
			log.Fatal(err)
		}
	}

	s.PurgeExpiredSecrets()
}

func (s *SecretsManagerInstance) publicKeyCacheKey(signature, databaseUuid string) string {
	return fmt.Sprintf("public_key:%s:%s", signature, databaseUuid)
}

func (s *SecretsManagerInstance) PurgeAccessKey(accessKeyId string) {
	s.cache("map").Forget(s.accessKeyCacheKey(accessKeyId))
	s.cache("transient").Forget(s.accessKeyCacheKey(accessKeyId))
}

func (s *SecretsManagerInstance) PurgeAccessKeys() {
	// Get all the file names in the access keys directory
	files, err := os.ReadDir(s.SecretsPath(config.Get("signature"), "access_keys"))

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

	err = os.WriteFile(
		s.SecretsPath(config.Get("signature"), fmt.Sprintf("access_keys/%s", accessKeyId)),
		[]byte(encryptedAccessKey),
		0666,
	)

	if err != nil {
		log.Fatal(err)
	}
}

func (s *SecretsManagerInstance) StoreDatabaseKey(
	databaseKey string,
	databaseUuid string,
) {
	format := "%s/%s/%s"

	filePaths := []string{
		fmt.Sprintf(format, Path(config.Get("signature")), "database_keys", databaseKey),
	}

	if config.Get("signature_next") != "" {
		filePaths = append(filePaths, fmt.Sprintf(format, Path(config.Get("signature_next")), "database_keys", databaseKey))
	}

	for _, filePath := range filePaths {
		if _, err := os.Stat(filepath.Dir(filePath)); os.IsNotExist(err) {
			os.MkdirAll(filepath.Dir(filePath), 0700)
		}

		err := os.WriteFile(
			filePath,
			[]byte(databaseUuid),
			0666,
		)

		if err != nil {
			log.Fatal(err)
		}
	}
}

func (s *SecretsManagerInstance) StoreDatabasePublicKey(signature, databaseUuid, publicKey string) error {
	if _, err := os.Stat(s.SecretsPath(signature, fmt.Sprintf("public_keys/%s", databaseUuid))); os.IsNotExist(err) {
		os.MkdirAll(s.SecretsPath(signature, fmt.Sprintf("public_keys/%s", databaseUuid)), 0755)
	}

	err := os.WriteFile(
		s.SecretsPath(signature, fmt.Sprintf("public_keys/%s/public_key", databaseUuid)),
		[]byte(publicKey),
		0666,
	)

	if err != nil {
		return err
	}

	return nil
}

func (s *SecretsManagerInstance) StoreDatabaseSettings(
	databaseUuid string,
	branchUuid string,
	databaseKey string,
	data string,
) error {
	for _, signature := range _auth.AllSignatures() {
		if _, err := os.Stat(s.SecretsPath(signature, fmt.Sprintf("settings/%s", databaseUuid))); os.IsNotExist(err) {
			os.MkdirAll(s.SecretsPath(signature, fmt.Sprintf("settings/%s", databaseUuid)), 0755)
		}

		decryptedSettingsData, err := SecretsManager().Decrypt(config.Get("signature"), data)

		if err != nil {
			if signature == config.Get("signature") {
				return err
			}

			log.Println(err)
			continue
		}

		var jsonSettings map[string]interface{}

		err = json.Unmarshal([]byte(decryptedSettingsData["value"]), &jsonSettings)

		if err != nil {
			if signature == config.Get("signature") {
				return err
			}

			log.Println(err)
			continue
		}

		jsonValue, err := json.Marshal(jsonSettings)

		if err != nil {
			if signature == config.Get("signature") {
				return err
			}

			log.Println(err)
			continue
		}

		encryptedSettings, err := SecretsManager().Encrypt(
			signature,
			string(jsonValue),
		)

		if err != nil {
			if signature == config.Get("signature") {
				return err
			}

			log.Println(err)
			continue
		}

		os.WriteFile(
			s.SecretsPath(signature, fmt.Sprintf("settings/%s/%s", databaseUuid, branchUuid)),
			[]byte(encryptedSettings),
			0666,
		)
	}

	return nil
}

func (s *SecretsManagerInstance) UpdateAccessKey(
	databaseUuid string,
	branchUuid string,
	accessKeyId string,
	privileges interface{},
) bool {
	accessKey, err := s.GetAccessKey(databaseUuid, accessKeyId)

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

	if err != nil {
		return false
	}

	jsonValue, err := json.Marshal(accessKey)

	if err != nil {
		log.Fatal(err)
	}

	os.WriteFile(
		s.SecretsPath(config.Get("signature"), fmt.Sprintf("access_keys/%s", accessKeyId)),
		jsonValue,
		0666,
	)

	return true
}
