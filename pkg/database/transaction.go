package database

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/cluster"
)

var ErrInvalidTransactionResponse = errors.New("invalid transaction response")
var ErrTransactionRolledBack = errors.New("transaction rolled back")

type Transaction struct {
	AccessKey        *auth.AccessKey
	cancel           context.CancelFunc
	context          context.Context
	closed           bool
	committed        bool
	cluster          *cluster.Cluster
	connection       *ClientConnection
	CreatedAt        time.Time
	databaseKey      *auth.DatabaseKey
	databaseManager  *DatabaseManager
	EndedAt          time.Time
	ID               string
	queryChannel     chan TransactionQuery
	rolledBack       bool
	StartedAt        time.Time
	responseChannel  chan *QueryResponse
	writesToDatabase bool
}

type TransactionQuery struct {
	query    *Query
	response *QueryResponse
}

func NewTransaction(
	cluster *cluster.Cluster,
	databaseManager *DatabaseManager,
	databaseKey *auth.DatabaseKey,
	accessKey *auth.AccessKey,
) (*Transaction, error) {
	connection, err := databaseManager.ConnectionManager().Get(
		databaseKey.DatabaseID,
		databaseKey.DatabaseBranchID,
	)

	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)

	transaction := &Transaction{
		AccessKey:       accessKey,
		cancel:          cancel,
		cluster:         cluster,
		context:         ctx,
		connection:      connection,
		databaseKey:     databaseKey,
		databaseManager: databaseManager,
		ID:              uuid.NewString(),
		CreatedAt:       time.Now().UTC(),
		queryChannel:    make(chan TransactionQuery, 1),
		responseChannel: make(chan *QueryResponse, 1),
		StartedAt:       time.Now().UTC(),
	}

	err = transaction.begin()

	if err != nil {
		log.Println("Error beginning transaction", err)
		return nil, err
	}

	go transaction.run()

	return transaction, nil
}

// Start a transaction on the database connection.
func (t *Transaction) begin() error {
	// Set connection timestamp before starting the transaction. This ensures we
	// have a consistent timestamp for the transaction and the vfs reads from
	// the proper WAL file and Page Log.
	t.connection.connection.setTimestamps()

	return t.connection.GetConnection().Begin()
}

// Close a transaction.
func (t *Transaction) Close() error {
	if t.closed {
		return nil
	}

	if !t.committed && !t.rolledBack {
		err := t.Rollback()

		if err != nil {
			return err
		}
	}

	t.connection.GetConnection().releaseTimestamps()
	t.closed = true
	t.cancel()

	t.databaseManager.ConnectionManager().Release(t.connection)

	close(t.queryChannel)
	close(t.responseChannel)

	return nil
}

// Commit the transaction. This will close the transaction and commit the
// changes to the database.
func (t *Transaction) Commit() error {
	if t.writesToDatabase {
		t.connection.GetConnection().committedAt = time.Now().UTC()
	}

	t.committed = true

	return t.connection.GetConnection().Commit()
}

// Rollback the transaction. This will close the transaction and rollback the
// changes to the database. If the transaction has already been committed, this
// will return an error.
func (t *Transaction) Rollback() error {
	defer func() {
		t.rolledBack = true
	}()

	return t.connection.GetConnection().Rollback()
}

// Run the transaction loop that listens for queries and processes them.
func (t *Transaction) run() {
	for {
		select {
		case <-t.context.Done():
			return
		case transactionQuery, ok := <-t.queryChannel:
			if !ok {
				// Channel is closed, exit the goroutine
				return
			}

			if transactionQuery.query.IsWrite() {
				t.writesToDatabase = true
			}

			response, err := transactionQuery.query.
				ForTransaction(t).
				Resolve(transactionQuery.response)

			if err != nil {
				log.Println("Error resolving query for transaction", err)
			}

			// Always try to send the response
			t.responseChannel <- response.(*QueryResponse)

			// After sending response, check if transaction should end
			if transactionQuery.query.IsTransactionEnd() || transactionQuery.query.IsTransactionRollback() {
				return
			}
		}
	}
}

// Resolve a query within the transaction.
func (t *Transaction) ResolveQuery(query *Query, response *QueryResponse) error {
	t.queryChannel <- TransactionQuery{
		query:    query,
		response: response,
	}

	<-t.responseChannel

	if response.id != query.Input.ID {
		return ErrInvalidTransactionResponse
	}

	return nil
}
