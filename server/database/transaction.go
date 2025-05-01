package database

import (
	"bytes"
	"context"
	"errors"
	"log"
	"time"

	"github.com/litebase/litebase/server/auth"

	"github.com/litebase/litebase/server/cluster"

	"github.com/google/uuid"
)

var ErrInvalidTransactionResponse = errors.New("invalid transaction response")
var ErrTransactionRolledBack = errors.New("transaction rolled back")

type Transaction struct {
	AccessKey        *auth.AccessKey
	cancel           context.CancelFunc
	context          context.Context
	closed           bool
	cluster          *cluster.Cluster
	connection       *ClientConnection
	CreatedAt        time.Time
	databaseKey      *auth.DatabaseKey
	databaseManager  *DatabaseManager
	EndedAt          time.Time
	Id               string
	queryChannel     chan TransactionQuery
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
		databaseKey.DatabaseId,
		databaseKey.BranchId,
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
		Id:              uuid.NewString(),
		CreatedAt:       time.Now(),
		queryChannel:    make(chan TransactionQuery, 1),
		responseChannel: make(chan *QueryResponse, 1),
		StartedAt:       time.Now(),
	}

	err = transaction.Begin()

	if err != nil {
		log.Println("Error beginning transaction", err)
		return nil, err
	}

	go transaction.run()

	return transaction, nil
}

func (t *Transaction) Begin() error {
	// Set connection timestamp before starting the transaction. This ensures we
	// have a consistent timestamp for the transaction and the vfs reads from
	// the proper WAL file and Page Log.
	t.connection.connection.setTimestamp()

	return t.connection.GetConnection().SqliteConnection().Begin()
}

// Close a transaction.
// TODO: If there are any queries that are still in progress, for example, a
// write transaction has locked the database and other transactions are waiting,
// the connection of the transaction should be interrupted.
func (t *Transaction) Close() {
	if t.closed {
		return
	}

	t.connection.GetConnection().releaseTimestamp()
	t.closed = true
	t.cancel()

	t.databaseManager.ConnectionManager().Release(
		t.databaseKey.DatabaseId,
		t.databaseKey.BranchId,
		t.connection,
	)

	close(t.queryChannel)
	close(t.responseChannel)
}

func (t *Transaction) Commit() error {
	defer t.Close()

	if t.writesToDatabase {
		t.connection.GetConnection().committedAt = time.Now()
	}

	return t.connection.GetConnection().SqliteConnection().Commit()
}

func (t *Transaction) Rollback() error {
	defer t.Close()

	return t.connection.GetConnection().SqliteConnection().Rollback()
}

func (t *Transaction) run() {
	for {
		select {
		case <-t.context.Done():
			return
		case transactionQuery := <-t.queryChannel:
			if transactionQuery.query.IsWrite() {
				t.writesToDatabase = true
			}

			response, err := transactionQuery.query.
				ForTransaction(t).
				Resolve(transactionQuery.response)

			if err != nil {
				log.Println("Error resolving query for transaction", err)
			}

			t.responseChannel <- response.(*QueryResponse)
		}
	}
}

func (t *Transaction) ResolveQuery(query *Query, response *QueryResponse) error {
	t.queryChannel <- TransactionQuery{
		query:    query,
		response: response,
	}

	<-t.responseChannel

	if !bytes.Equal(response.id, query.Input.Id) {
		return ErrInvalidTransactionResponse
	}

	return nil
}
