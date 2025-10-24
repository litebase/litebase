package auth

import (
	"log/slog"
	"time"
)

type AccessKey struct {
	ID              int64       `json:"id"`
	AccessKeyID     string      `json:"accessKeyId"`
	AccessKeySecret string      `json:"accessKeySecret"`
	Description     string      `json:"description"`
	CreatedAt       time.Time   `json:"createdAt"`
	UpdatedAt       time.Time   `json:"updatedAt"`
	Statements      []Statement `json:"statements"`

	AccessKeyManager *AccessKeyManager `json:"-"`
}

type AccessKeyResponse struct {
	AccessKeyID string      `json:"accessKeyId"`
	Description string      `json:"description"`
	CreatedAt   time.Time   `json:"createdAt"`
	UpdatedAt   time.Time   `json:"updatedAt"`
	Statements  []Statement `json:"statements"`
}

// Create a new AccessKey instance.
func NewAccessKey(
	accessKeyManager *AccessKeyManager,
	accessKeyId string,
	accessKeySecret string,
	description string,
	statements []Statement,
) *AccessKey {
	return &AccessKey{
		AccessKeyManager: accessKeyManager,
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

// Delete the AccessKey from storage.
func (accessKey *AccessKey) Delete() error {
	err := accessKey.AccessKeyManager.accessKeyStorage.Delete(accessKey.AccessKeyID)

	if err != nil {
		return err
	}

	err = accessKey.AccessKeyManager.Purge(accessKey.AccessKeyID)

	if err != nil {
		slog.Error("failed to purge access key", "error", err)
	}

	return nil
}

// Rotate the access key.
func (accessKey *AccessKey) Rotate() error {
	return accessKey.AccessKeyManager.accessKeyStorage.UpdateNext(accessKey)
}

// Return the response representation of the AccessKey.
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
	statements []Statement,
) error {
	accessKey.Description = description
	accessKey.Statements = statements
	accessKey.UpdatedAt = time.Now().UTC()

	err := accessKey.AccessKeyManager.accessKeyStorage.Update(accessKey)

	if err != nil {
		return err
	}

	return accessKey.AccessKeyManager.Purge(accessKey.AccessKeyID)
}
