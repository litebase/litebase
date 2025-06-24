package auth

import (
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
)

type AccessKey struct {
	AccessKeyId      string `json:"access_key_id"`
	AccessKeySecret  string `json:"access_key_secret"`
	Description      string `json:"description"`
	accessKeyManager *AccessKeyManager
	CreatedAt        int64                `json:"created_at"`
	UpdatedAt        int64                `json:"updated_at"`
	Statements       []AccessKeyStatement `json:"statements"`
}

// Create a new AccessKey instance.
func NewAccessKey(
	accessKeyManager *AccessKeyManager,
	accessKeyId string,
	accessKeySecret string,
	description string,
	statements []AccessKeyStatement,
) *AccessKey {
	return &AccessKey{
		accessKeyManager: accessKeyManager,
		AccessKeyId:      accessKeyId,
		AccessKeySecret:  accessKeySecret,
		Description:      description,
		Statements:       statements,
	}
}

func (accessKey *AccessKey) AuthorizeForResource(resources []string, actions []Privilege) bool {
	hasAuthorization := false

	for _, action := range actions {
		for _, resource := range resources {
			if Authorized(accessKey.Statements, resource, action) {
				hasAuthorization = true
				break // No need to check further if one action is authorized
			}
		}
	}

	return hasAuthorization
}

// Delete the AccessKey from the filesystem.
func (accessKey *AccessKey) Delete() error {
	keys := AllKeys(
		accessKey.accessKeyManager.objectFS,
	)

	for _, key := range keys {
		path := fmt.Sprintf("%s/access_keys/%s", key, accessKey.AccessKeyId)

		err := accessKey.accessKeyManager.objectFS.Remove(path)

		if err != nil {
			if !os.IsNotExist(err) {
				return err
			}
		}
	}

	err := accessKey.accessKeyManager.Purge(accessKey.AccessKeyId)

	if err != nil {
		slog.Error("failed to purge access key", "error", err)
	}

	accessKey = nil

	return nil
}

// Update the AccessKey statements.
func (accessKey *AccessKey) Update(
	description string,
	statements []AccessKeyStatement,
) error {
	accessKey.Description = description
	accessKey.Statements = statements

	jsonValue, err := json.Marshal(accessKey)

	if err != nil {
		log.Println(err)
		return err
	}

	encryptedAccessKey, err := accessKey.accessKeyManager.auth.SecretsManager.Encrypt(
		accessKey.accessKeyManager.config.EncryptionKey,
		jsonValue,
	)

	if err != nil {
		return err
	}

	err = accessKey.accessKeyManager.objectFS.WriteFile(
		accessKey.accessKeyManager.auth.SecretsManager.SecretsPath(
			accessKey.accessKeyManager.config.EncryptionKey,
			fmt.Sprintf("access_keys/%s", accessKey.AccessKeyId),
		),
		[]byte(encryptedAccessKey),
		0600,
	)

	if err != nil {
		log.Println(err)

		return err
	}

	return accessKey.accessKeyManager.Purge(accessKey.AccessKeyId)
}
