package database_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
)

func TestTransactionManager(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		t.Run("NewTransactionManager", func(t *testing.T) {
			manager := app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).TransactionManager()

			if manager == nil {
				t.Fatal("expected transaction manager to be created")
			}

			if manager.DatabaseID != mock.DatabaseID || manager.BranchID != mock.DatabaseBranchID {
				t.Fatalf("expected transaction manager with db: %s, branch: %s, got db: %s, branch: %s",
					mock.DatabaseID, mock.DatabaseBranchID, manager.DatabaseID, manager.BranchID)
			}
		})

		t.Run("CreateGetAndRemoveTransaction", func(t *testing.T) {
			manager := app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).TransactionManager()

			transaction, err := manager.Create(app.Cluster, app.DatabaseManager, mock.DatabaseKey, mock.Credential)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			if transaction == nil {
				t.Fatal("expected transaction to be created")
			}

			fetchedTransaction, err := manager.Get(transaction.ID)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			if fetchedTransaction == nil {
				t.Fatal("expected transaction to be fetched")
			}

			if fetchedTransaction.ID != transaction.ID {
				t.Fatalf("expected fetched transaction ID to be %s, got %s",
					transaction.ID, fetchedTransaction.ID)
			}

			manager.Remove(transaction.ID)

			_, err = manager.Get(transaction.ID)

			if err == nil {
				t.Fatal("expected error, got none")
			}

			if err != database.ErrTransactionNotFound {
				t.Fatalf("expected error to be %v, got %v", database.ErrTransactionNotFound, err)
			}
		})

		t.Run("Shutdown", func(t *testing.T) {
			manager := app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).TransactionManager()

			transaction, err := manager.Create(app.Cluster, app.DatabaseManager, mock.DatabaseKey, mock.Credential)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			if transaction == nil {
				t.Fatal("expected transaction to be created")
			}

			manager.Shutdown()

			_, err = manager.Get(transaction.ID)

			if err == nil {
				t.Fatal("expected error, got none")
			}

			if err != database.ErrTransactionNotFound {
				t.Fatalf("expected error to be %v, got %v", database.ErrTransactionNotFound, err)
			}
		})
	})
}
