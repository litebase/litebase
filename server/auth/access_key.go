package auth

import (
	"encoding/json"
	"fmt"
	_auth "litebase/internal/auth"
	"log"
	"os"
)

type AccessKey struct {
	AccessKeyId      string `json:"access_key_id"`
	AccessKeySecret  string `json:"access_key_secret"`
	accessKeyManager *AccessKeyManager
	CreatedAt        int64                  `json:"created_at"`
	UpdatedAt        int64                  `json:"updated_at"`
	Permissions      []*AccessKeyPermission `json:"permissions"`
}

// Create a new AccessKey instance.
func NewAccessKey(
	accessKeyManager *AccessKeyManager,
	accessKeyId string,
	accessKeySecret string,
	permissions []*AccessKeyPermission,
) *AccessKey {
	return &AccessKey{
		accessKeyManager: accessKeyManager,
		AccessKeyId:      accessKeyId,
		AccessKeySecret:  accessKeySecret,
		Permissions:      permissions,
	}
}

// Delete the AccessKey from the filesystem.
func (accessKey *AccessKey) Delete() error {
	signatures := _auth.AllSignatures(
		accessKey.accessKeyManager.objectFS,
	)

	for _, signature := range signatures {
		path := fmt.Sprintf("%s/access_keys/%s", signature, accessKey.AccessKeyId)

		err := accessKey.accessKeyManager.objectFS.Remove(path)

		if err != nil {
			if !os.IsNotExist(err) {
				return err
			}
		}
	}

	accessKey.accessKeyManager.Purge(accessKey.AccessKeyId)

	accessKey = nil

	return nil
}

// Update the AccessKey permissions.
func (accessKey *AccessKey) Update(
	permissions []*AccessKeyPermission,
) error {
	accessKey.Permissions = permissions

	jsonValue, err := json.Marshal(accessKey)

	if err != nil {
		log.Println(err)
		return err
	}

	encryptedAccessKey, err := accessKey.accessKeyManager.auth.SecretsManager.Encrypt(
		accessKey.accessKeyManager.config.Signature,
		jsonValue,
	)

	if err != nil {
		return err
	}

	err = accessKey.accessKeyManager.objectFS.WriteFile(
		accessKey.accessKeyManager.auth.SecretsManager.SecretsPath(
			accessKey.accessKeyManager.config.Signature,
			fmt.Sprintf("access_keys/%s", accessKey.AccessKeyId),
		),
		[]byte(encryptedAccessKey),
		0644,
	)

	if err != nil {
		log.Println(err)

		return err
	}

	return accessKey.accessKeyManager.Purge(accessKey.AccessKeyId)
}
