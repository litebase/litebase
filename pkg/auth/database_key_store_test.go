package auth_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/server"
)

func TestDatabaseKeyStore(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("NewDatabaseKeyStore", func(t *testing.T) {
			databaseKeyStore, err := auth.NewDatabaseKeyStore(
				app.Cluster.TmpTieredFS(),
				"test-database-key-store",
			)

			if err != nil {
				t.Fatal(err)
			}

			if databaseKeyStore == nil {
				t.Fatal("database key store is nil")
			}
		})

		t.Run("All", func(t *testing.T) {
			databaseKeyStore, err := auth.NewDatabaseKeyStore(
				app.Cluster.TmpTieredFS(),
				"test-database-key-store",
			)

			if err != nil {
				t.Fatal(err)
			}

			var keys []*auth.DatabaseKey

			for i := range 4 {
				mock := test.MockDatabase(app)

				err = databaseKeyStore.Put(mock.DatabaseKey)

				if err != nil {
					t.Fatalf("failed to put key %d: %v", i, err)
				}

				keys = append(keys, mock.DatabaseKey)
			}

			var allKeys []*auth.DatabaseKey

			for key := range databaseKeyStore.All() {
				allKeys = append(allKeys, key)
			}

			if len(allKeys) != 4 {
				t.Fatalf("expected 4 keys, got %d", len(allKeys))
			}

			keyMap := make(map[string]bool)

			for _, k := range allKeys {
				keyMap[k.Key] = true
			}

			for _, k := range keys {
				if !keyMap[k.Key] {
					t.Fatalf("key %s not found in allKeys", k.Key)
				}
			}
		})

		t.Run("Close", func(t *testing.T) {
			databaseKeyStore, err := auth.NewDatabaseKeyStore(
				app.Cluster.TmpTieredFS(),
				"test-database-key-store",
			)

			if err != nil {
				t.Fatal(err)
			}

			err = databaseKeyStore.Close()

			if err != nil {
				t.Fatal(err)
			}
		})

		t.Run("PutAndGet", func(t *testing.T) {
			mock := test.MockDatabase(app)

			databaseKeyStore, err := auth.NewDatabaseKeyStore(
				app.Cluster.TmpTieredFS(),
				"test-database-key-store",
			)

			if err != nil {
				t.Fatal(err)
			}

			err = databaseKeyStore.Put(mock.DatabaseKey)

			if err != nil {
				t.Fatal(err)
			}

			retrievedKey, err := databaseKeyStore.Get(mock.DatabaseKey.Key)

			if err != nil {
				t.Fatal(err)
			}

			if retrievedKey == nil || retrievedKey.Key != mock.DatabaseKey.Key {
				t.Fatal("retrieved key is nil or does not match")
			}
		})

		t.Run("Delete", func(t *testing.T) {
			mock := test.MockDatabase(app)

			databaseKeyStore, err := auth.NewDatabaseKeyStore(
				app.Cluster.TmpTieredFS(),
				"test-database-key-store",
			)

			if err != nil {
				t.Fatal(err)
			}

			err = databaseKeyStore.Put(mock.DatabaseKey)

			if err != nil {
				t.Fatal(err)
			}

			err = databaseKeyStore.Delete(mock.DatabaseKey.Key)

			if err != nil {
				t.Fatal(err)
			}

			retrievedKey, err := databaseKeyStore.Get(mock.DatabaseKey.Key)

			if err == nil || retrievedKey != nil {
				t.Fatal("expected error or nil key after deletion")
			}
		})

		t.Run("PutMany", func(t *testing.T) {
			auth.DefaultDatabaseKeyStoreCacheSize = 2

			databaseKeyStore, err := auth.NewDatabaseKeyStore(
				app.Cluster.TmpTieredFS(),
				"test-database-key-store",
			)

			if err != nil {
				t.Fatal(err)
			}

			for range 200 {
				mock := test.MockDatabase(app)

				err = databaseKeyStore.Put(mock.DatabaseKey)

				if err != nil {
					t.Fatal(err)
				}

				retrievedKey, err := databaseKeyStore.Get(mock.DatabaseKey.Key)

				if err != nil {
					t.Fatal(err)
				}

				if retrievedKey == nil || retrievedKey.Key != mock.DatabaseKey.Key {
					t.Fatal("retrieved key is nil or does not match")
				}
			}
		})

		t.Run("DeleteAndReuseKeySpace", func(t *testing.T) {
			databaseKeyStore, err := auth.NewDatabaseKeyStore(
				app.Cluster.TmpTieredFS(),
				"test-database-key-store-reuse",
			)

			if err != nil {
				t.Fatal(err)
			}

			if databaseKeyStore.Len() != 0 {
				t.Fatalf("expected 0 keys got %d", databaseKeyStore.Len())
			}

			mock := test.MockDatabase(app)

			err = databaseKeyStore.Put(mock.DatabaseKey)

			if err != nil {
				t.Fatal(err)
			}

			if databaseKeyStore.Len() != 1 {
				t.Fatalf("expected 1 key got %d", databaseKeyStore.Len())
			}

			err = databaseKeyStore.Delete(mock.DatabaseKey.Key)

			if err != nil {
				t.Fatal(err)
			}

			err = databaseKeyStore.Put(mock.DatabaseKey)

			if err != nil {
				t.Fatal(err)
			}

			if databaseKeyStore.Len() != 1 {
				t.Fatalf("expected 1 key got %d", databaseKeyStore.Len())
			}
		})
	})
}
