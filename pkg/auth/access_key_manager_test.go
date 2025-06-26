package auth_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/server"
)

func TestAccessKeyManager(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("NewAccessKeyManager", func(t *testing.T) {
			a := auth.NewAuth(
				app.Config,
				app.Cluster.NetworkFS(),
				app.Cluster.ObjectFS(),
				app.Cluster.TmpFS(),
				app.Cluster.TmpTieredFS(),
			)

			akm := auth.NewAccessKeyManager(a, a.Config, a.ObjectFS)

			if akm == nil {
				t.Error("Expected NewAccessKeyManager to return a non-nil AccessKeyManager")
			}
		})

		t.Run("AllAccessKeys", func(t *testing.T) {
			akm := app.Auth.AccessKeyManager

			for i := range 10 {
				akm.Create(fmt.Sprintf("Description %d", i), []auth.AccessKeyStatement{{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}}})
			}

			accessKeys, err := akm.AllAccessKeyIds()

			if err != nil {
				t.Error("Expected AllAccessKeyIds to return an empty slice of strings")
			}

			if len(accessKeys) != 10 {
				t.Error("Expected AllAccessKeyIds to return 10 access keys")
			}
		})

		t.Run("Create", func(t *testing.T) {
			accessKey, err := app.Auth.AccessKeyManager.Create("Test access key", []auth.AccessKeyStatement{{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}}})

			if err != nil {
				t.Error("Expected Create to return a non-nil error")
			}

			if accessKey == nil {
				t.Fatal("Expected Create to return a non-nil AccessKey")
			}

			if accessKey.AccessKeyID == "" {
				t.Error("Expected AccessKeyID to not be an empty string")
			}

			if accessKey.AccessKeySecret == "" {
				t.Error("Expected AccessKeySecret to not be an empty string")
			}
		})

		t.Run("GenerateAccessKeyId", func(t *testing.T) {
			accessKeyId, err := app.Auth.AccessKeyManager.GenerateAccessKeyId()

			if err != nil {
				t.Error("Expected GenerateAccessKeyId to return a non-nil error")
			}

			if accessKeyId == "" {
				t.Error("Expected GenerateAccessKeyId to not return an empty string")
			}
		})

		t.Run("GenerateAccessKeySecret", func(t *testing.T) {
			accessKeySecret := app.Auth.AccessKeyManager.GenerateAccessKeySecret()

			if accessKeySecret == "" {
				t.Error("Expected GenerateAccessKeySecret to not return an empty string")
			}
		})

		t.Run("Get", func(t *testing.T) {
			accessKey, err := app.Auth.AccessKeyManager.Create("Test access key", []auth.AccessKeyStatement{{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}}})

			if err != nil {
				t.Error("Expected Create to return a non-nil error")
			}

			if accessKey == nil {
				t.Fatal("Expected Create to return a non-nil AccessKey")
			}

			accessKey2, err := app.Auth.AccessKeyManager.Get(accessKey.AccessKeyID)

			if err != nil {
				t.Error("Expected Get to return a non-nil error")
			}

			if accessKey2 == nil {
				t.Fatal("Expected Get to return a non-nil AccessKey")
			}

			if accessKey2.AccessKeyID != accessKey.AccessKeyID {
				t.Error("Expected AccessKeyID to match")
			}

			if accessKey2.AccessKeySecret != accessKey.AccessKeySecret {
				t.Error("Expected AccessKeySecret to match")
			}
		})

		t.Run("Purge", func(t *testing.T) {
			server1 := test.NewTestServer(t)
			defer server1.Shutdown()

			server2 := test.NewTestServer(t)
			defer server2.Shutdown()

			accessKey, err := server1.App.Auth.AccessKeyManager.Create("Test access key", []auth.AccessKeyStatement{{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}}})

			if err != nil {
				t.Error("Expected Create to return a non-nil error")
			}

			if accessKey == nil {
				t.Fatal("Expected Create to return a non-nil AccessKey")
			}

			accessKey1, err := server1.App.Auth.AccessKeyManager.Get(accessKey.AccessKeyID)

			if err != nil {
				t.Error("Expected Get to return a non-nil error")
			}

			if accessKey1 == nil {
				t.Fatal("Expected Get to return a non-nil AccessKey")
			}

			accessKey2, err := server2.App.Auth.AccessKeyManager.Get(accessKey.AccessKeyID)

			if err != nil {
				t.Error("Expected Get to return a non-nil error")
			}

			if accessKey2 == nil {
				t.Fatal("Expected Get to return a non-nil AccessKey")
			}

			err = accessKey.Delete()

			if err != nil {
				t.Error("Expected Delete to return a non-nil error")
			}

			err = server1.App.Auth.AccessKeyManager.Purge(accessKey.AccessKeyID)

			if err != nil {
				t.Error("Expected Purge to return a non-nil error")
			}

			accessKey1, err = server1.App.Auth.AccessKeyManager.Get(accessKey.AccessKeyID)

			if err == nil {
				t.Error("Expected Get to return an error after Purge")
			}

			if accessKey1 != nil {
				t.Error("Expected Get to return a nil AccessKey after Purge")
			}

			accessKey2, err = server2.App.Auth.AccessKeyManager.Get(accessKey.AccessKeyID)

			if err == nil {
				t.Error("Expected Get to return an error after Purge")
			}

			if accessKey2 != nil {
				t.Error("Expected Get to return a nil AccessKey after Purge")
			}
		})

		t.Run("PurgeAll", func(t *testing.T) {
			for i := 0; i < 10; i++ {
				app.Auth.AccessKeyManager.Create(
					fmt.Sprintf("Test access key %d", i),
					[]auth.AccessKeyStatement{{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}}},
				)
			}

			err := app.Auth.AccessKeyManager.PurgeAll()

			if err != nil {
				t.Error("Expected PurgeAll to return a non-nil error")
			}
		})
	})
}
