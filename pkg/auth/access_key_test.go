package auth_test

import (
	"encoding/json"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/server"
)

func TestAccessKey(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("NewAccessKey", func(t *testing.T) {
			accessKey := auth.NewAccessKey(
				app.Auth.AccessKeyManager,
				"accessKeyId",
				"accessKeySecret",
				"Description",
				[]auth.AccessKeyStatement{},
			)

			if accessKey == nil {
				t.Fatal("Expected accessKey to be non-nil")
			}

			if accessKey.AccessKeyId != "accessKeyId" {
				t.Errorf("Expected accessKeyId to be 'accessKeyId', got %s", accessKey.AccessKeyId)
			}

			if accessKey.AccessKeySecret != "accessKeySecret" {
				t.Errorf("Expected accessKeySecret to be 'accessKeySecret', got %s", accessKey.AccessKeySecret)
			}

			if accessKey.Description != "Description" {
				t.Errorf("Expected description to be 'Description', got %s", accessKey.Description)
			}

			if accessKey.CreatedAt.IsZero() {
				t.Error("Expected CreatedAt to be set, got zero value")
			}

			if accessKey.UpdatedAt.IsZero() {
				t.Error("Expected UpdatedAt to be set, got zero value")
			}
		})

		t.Run("AccessKeyResponse", func(t *testing.T) {
			accessKey := auth.NewAccessKey(
				app.Auth.AccessKeyManager,
				"accessKeyId",
				"accessKeySecret",
				"Description",
				[]auth.AccessKeyStatement{},
			)

			jsonData, err := json.Marshal(accessKey.ToResponse())

			if err != nil {
				t.Error(err)
			}

			if jsonData == nil {
				t.Error("Expected JSON data to be non-empty")
			}

			var result map[string]any

			if err := json.Unmarshal(jsonData, &result); err != nil {
				t.Errorf("Failed to unmarshal JSON: %v", err)
			}

			if result["access_key_id"] != "accessKeyId" {
				t.Errorf("Expected accessKeyId to be 'accessKeyId', got %v", result["access_key_id"])
			}

			if _, ok := result["access_key_secret"]; ok {
				t.Error("Expected accessKeySecret to be omitted from JSON, but it was included")
			}
		})

		t.Run("DeleteAccessKey", func(t *testing.T) {
			accessKey := auth.NewAccessKey(
				app.Auth.AccessKeyManager,
				"accessKeyId",
				"accessSecret",
				"",
				[]auth.AccessKeyStatement{},
			)

			err := app.Auth.SecretsManager.StoreAccessKey(accessKey)

			if err != nil {
				t.Error(err)
			}

			if err := accessKey.Delete(); err != nil {
				t.Error(err)
			}

			accessKey, err = app.Auth.AccessKeyManager.Get("accessKeyId")

			if err == nil {
				t.Error("Expected accessKey to be nil")
			}

			if accessKey != nil {
				t.Errorf("Expected accessKey to be nil, got %v", accessKey)
			}
		})

		t.Run("UpdateAccessKey", func(t *testing.T) {
			accessKey := auth.NewAccessKey(
				app.Auth.AccessKeyManager,
				"accessKeyId",
				"accessSecret",
				"Description",
				[]auth.AccessKeyStatement{},
			)

			err := app.Auth.SecretsManager.StoreAccessKey(accessKey)

			if err != nil {
				t.Error(err)
			}

			statements := []auth.AccessKeyStatement{
				{
					Resource: "*",
					Actions:  []auth.Privilege{"*"},
				},
			}

			updatedAt := accessKey.UpdatedAt

			if err := accessKey.Update("Updated Description", statements); err != nil {
				t.Error(err)
			}

			accessKey, err = app.Auth.AccessKeyManager.Get("accessKeyId")

			if err != nil {
				t.Error(err)
			}

			if accessKey == nil {
				t.Fatal("Expected accessKey to be non-nil")
			}

			if accessKey.Description != "Updated Description" {
				t.Errorf("Expected description to be 'Updated Description', got %s", accessKey.Description)
			}

			if len(accessKey.Statements) != 1 {
				t.Errorf("Expected statements to have length 1, got %d", len(accessKey.Statements))
			}

			if accessKey.Statements[0].Resource != "*" {
				t.Errorf("Expected resource to be '*', got %s", accessKey.Statements[0].Resource)
			}

			if len(accessKey.Statements[0].Actions) != 1 {
				t.Errorf("Expected actions to have length 1, got %d", len(accessKey.Statements[0].Actions))
			}

			if accessKey.Statements[0].Actions[0] != "*" {
				t.Errorf("Expected action to be '*', got %s", accessKey.Statements[0].Actions[0])
			}

			if accessKey.UpdatedAt.Equal(updatedAt) {
				t.Error("Expected UpdatedAt to be updated, but it is still the same as before")
			}
		})
	})
}
