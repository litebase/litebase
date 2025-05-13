package auth_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server"
	"github.com/litebase/litebase/server/auth"
)

func TestNewDatabaseKeyStore(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
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
}

func TestDatabaseKeyStore_Close(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
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
}

func TestDatabaseKeyStore_PutAndGet(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
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
}

func TestDatabaseKeyStore_Delete(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
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
}

func TestDatabaseKeyStore_PutMany(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
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
}

func TestDatabaseKeyStore_DeleteAndReuseKeySpace(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		databaseKeyStore, err := auth.NewDatabaseKeyStore(
			app.Cluster.TmpTieredFS(),
			"test-database-key-store",
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
}
