package database

import (
	"fmt"
	"sync"
)

var (
	txnMu  sync.RWMutex
	txnMap = make(map[string]struct{})
)

func txnKey(databaseID, branchID string) string {
	return fmt.Sprintf("%s:%s", databaseID, branchID)
}

// RegisterActiveTransaction marks a transaction as active for the database+branch.
func RegisterActiveTransaction(databaseID, branchID string) {
	txnMu.Lock()
	defer txnMu.Unlock()

	txnMap[txnKey(databaseID, branchID)] = struct{}{}
}

// UnregisterActiveTransaction removes the active transaction mark.
func UnregisterActiveTransaction(databaseID, branchID string) {
	txnMu.Lock()
	defer txnMu.Unlock()

	delete(txnMap, txnKey(databaseID, branchID))
}

// IsTransactionActive returns true if a transaction is active for the database+branch.
func IsTransactionActive(databaseID, branchID string) bool {
	txnMu.RLock()
	defer txnMu.RUnlock()

	_, ok := txnMap[txnKey(databaseID, branchID)]

	return ok
}
