package auth

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"
)

type AccessKey struct {
	AccessKeyID      string `json:"access_key_id"`
	AccessKeySecret  string `json:"access_key_secret"`
	Description      string `json:"description"`
	accessKeyManager *AccessKeyManager
	CreatedAt        time.Time            `json:"created_at"`
	UpdatedAt        time.Time            `json:"updated_at"`
	Statements       []AccessKeyStatement `json:"statements"`

	hash [32]byte
}

type AccessKeyResponse struct {
	AccessKeyID string               `json:"access_key_id"`
	Description string               `json:"description"`
	CreatedAt   time.Time            `json:"created_at"`
	UpdatedAt   time.Time            `json:"updated_at"`
	Statements  []AccessKeyStatement `json:"statements"`
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
		AccessKeyID:      accessKeyId,
		AccessKeySecret:  accessKeySecret,
		Description:      description,
		Statements:       statements,
		CreatedAt:        time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
	}
}

// Determine if the AccessKey has authorization for the given resources and actions.
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
		path := fmt.Sprintf("%s/access_keys/%s", key, accessKey.AccessKeyID)

		err := accessKey.accessKeyManager.objectFS.Remove(path)

		if err != nil {
			if !os.IsNotExist(err) {
				return err
			}
		}
	}

	err := accessKey.accessKeyManager.Purge(accessKey.AccessKeyID)

	if err != nil {
		slog.Error("failed to purge access key", "error", err)
	}

	accessKey = nil

	return nil
}

// Return the hash of the AccessKey.
func (accessKey *AccessKey) Hash() [32]byte {
	if accessKey.hash != [32]byte{} {
		return accessKey.hash
	}

	accessKey.updateHash()

	return accessKey.hash
}

func (accessKey *AccessKey) ToResponse() *AccessKeyResponse {
	return &AccessKeyResponse{
		AccessKeyID: accessKey.AccessKeyID,
		Description: accessKey.Description,
		CreatedAt:   accessKey.CreatedAt,
		UpdatedAt:   accessKey.UpdatedAt,
		Statements:  accessKey.Statements,
	}
}

// Update the AccessKey statements.
func (accessKey *AccessKey) Update(
	description string,
	statements []AccessKeyStatement,
) error {
	accessKey.Description = description
	accessKey.Statements = statements
	accessKey.UpdatedAt = time.Now().UTC()

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
			fmt.Sprintf("access_keys/%s", accessKey.AccessKeyID),
		),
		[]byte(encryptedAccessKey),
		0600,
	)

	if err != nil {
		log.Println(err)

		return err
	}

	accessKey.updateHash()

	return accessKey.accessKeyManager.Purge(accessKey.AccessKeyID)
}

// Update the internal hash of the access key.
func (accessKey *AccessKey) updateHash() {
	jsonBytes, err := json.Marshal(accessKey)
	if err != nil {
		return
	}

	accessKey.hash = sha256.Sum256(jsonBytes)
}
