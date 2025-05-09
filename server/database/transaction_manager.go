package database

import (
	"errors"
	"sync"

	"github.com/litebase/litebase/server/auth"
	"github.com/litebase/litebase/server/cluster"
)

type TransactionManager struct {
	BranchId     string
	DatabaseId   string
	mutex        *sync.RWMutex
	transactions map[string]*Transaction
}

var ErrTransactionNotFound = errors.New("transaction not found")

func NewTransactionManager(databaseId, branchId string) *TransactionManager {
	return &TransactionManager{
		BranchId:     branchId,
		DatabaseId:   databaseId,
		mutex:        &sync.RWMutex{},
		transactions: make(map[string]*Transaction),
	}
}

func (d *TransactionManager) Create(
	cluster *cluster.Cluster,
	databaseManager *DatabaseManager,
	databaseKey *auth.DatabaseKey,
	accessKey *auth.AccessKey,
) (*Transaction, error) {
	transaction, err := NewTransaction(
		cluster,
		databaseManager,
		databaseKey,
		accessKey,
	)

	if err != nil {
		return nil, err
	}

	d.mutex.Lock()
	d.transactions[transaction.Id] = transaction
	d.mutex.Unlock()

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

	d.transactions[transactionId].Close()

	delete(d.transactions, transactionId)
}

// Close all open transactions.
func (d *TransactionManager) Shutdown() {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	for _, transaction := range d.transactions {
		transaction.Close()
	}

	d.transactions = make(map[string]*Transaction)
}
