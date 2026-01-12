package database

import (
	"errors"
	"log/slog"
	"sync"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/cluster"
)

type TransactionManager struct {
	Branch       *Branch
	mutex        *sync.RWMutex
	transactions map[string]*Transaction
}

var ErrTransactionNotFound = errors.New("transaction not found")

// Create a new instance of a TransactionManager.
func NewTransactionManager(branch *Branch) *TransactionManager {
	return &TransactionManager{
		Branch:       branch,
		mutex:        &sync.RWMutex{},
		transactions: make(map[string]*Transaction),
	}
}

// Create a new instance of a transaction.
func (d *TransactionManager) Create(
	cluster *cluster.Cluster,
	databaseManager *DatabaseManager,
	databaseKey *auth.DatabaseKey,
	credential *auth.Credential,
) (*Transaction, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	transaction, err := NewTransaction(
		cluster,
		databaseManager,
		databaseKey,
		credential,
	)

	if err != nil {
		return nil, err
	}

	d.transactions[transaction.ID] = transaction

	return transaction, nil
}

// Return a transaction by its ID. If the transaction is not found, return an error.
func (d *TransactionManager) Get(transactionId string) (*Transaction, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	transaction, ok := d.transactions[transactionId]

	if !ok {
		return nil, ErrTransactionNotFound
	}

	return transaction, nil
}

// Remove a transaction by its ID. This will also close the transaction if the
// transaction is still open.
func (d *TransactionManager) Remove(transactionId string) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if err := d.transactions[transactionId].Close(); err != nil {
		slog.Error("Error closing transaction", "error", err)
	}

	delete(d.transactions, transactionId)
}

// Close all open transactions.
func (d *TransactionManager) Shutdown() {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	for _, transaction := range d.transactions {
		if err := transaction.Close(); err != nil {
			slog.Error("Error closing transaction", "error", err)
		}
	}

	d.transactions = make(map[string]*Transaction)
}
