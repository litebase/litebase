package database

// import (
// 	"errors"
// 	"github.com/litebase/litebase/common/config"
// 	"github.com/litebase/litebase/server/cluster"
// 	"github.com/litebase/litebase/server/storage"
// 	"log"
// 	"sync"
// )

// type DatabaseWALIndex struct {
// 	BranchId          string
// 	config            *config.Config
// 	connectionManager *ConnectionManager
// 	DatabaseId        string
// 	mutext            *sync.RWMutex
// 	node              *cluster.Node
// 	// currentTimestamp  int64
// 	// timestamps        []int64
// 	walIndex    *storage.WALIndex
// 	walVersions map[int64]*DatabaseWAL
// }

// func NewDatabaseWALIndex(
// 	config *config.Config,
// 	node *cluster.Node,
// 	connectionManager *ConnectionManager,
// 	databaseId,
// 	branchId string,
// ) *DatabaseWALIndex {
// 	return &DatabaseWALIndex{
// 		BranchId:          branchId,
// 		config:            config,
// 		connectionManager: connectionManager,
// 		DatabaseId:        databaseId,
// 		mutext:            &sync.RWMutex{},
// 		node:              node,
// 		walIndex: storage.NewWALIndex(
// 			config,
// 			databaseId,
// 			branchId,
// 			node.Cluster.RemoteFS(),
// 		),
// 	}
// }

// // func (index *DatabaseWALIndex) GetLatestTimestamp() int64 {
// // 	index.mutext.RLock()
// // 	defer index.mutext.RUnlock()

// // 	if len(index.timestamps) == 0 {
// // 		return 0
// // 	}

// // 	return index.timestamps[len(index.timestamps)-1]
// // }

// // func (index *DatabaseWALIndex) SetCurrentTimestamp(timestamp int64) {
// // 	index.mutext.Lock()
// // 	defer index.mutext.Unlock()

// // 	index.currentTimestamp = timestamp
// // 	// log.Println("Set current timestamp", index.currentTimestamp)

// // 	if index.node.IsReplica() {
// // 		return
// // 	}

// // 	// index.node.Primary().Publish(messages.NodeMessage{
// // 	// 	Data: messages.WALIndexTimestampMessage{
// // 	// 		BranchId:   index.BranchId,
// // 	// 		DatabaseId: index.DatabaseId,
// // 	// 		Timestamp:  index.currentTimestamp,
// // 	// 	},
// // 	// })
// // }

// func (index *DatabaseWALIndex) SetHeader(header []byte) error {
// 	index.mutext.Lock()
// 	defer index.mutext.Unlock()

// 	if index.node.IsPrimary() {
// 		return errors.New("cannot set header on primary node")
// 	}

// 	connection, err := index.connectionManager.Get(index.DatabaseId, index.BranchId)

// 	if err != nil {
// 		return err
// 	}

// 	defer index.connectionManager.Release(index.DatabaseId, index.BranchId, connection)

// 	err = connection.GetConnection().vfs.SetShmHeader(header)

// 	if err != nil {
// 		log.Println("Failed to set header", err)

// 		return err
// 	}

// 	return nil
// }

// // func (index *DatabaseWALIndex) SetTimestamps(timestamps []int64) {
// // 	index.mutext.Lock()
// // 	defer index.mutext.Unlock()

// // 	index.timestamps = timestamps
// // }

// // func (index *DatabaseWALIndex) Timestamp() int64 {
// // 	index.mutext.RLock()
// // 	defer index.mutext.RUnlock()

// // 	return index.currentTimestamp
// // }

// func (w *DatabaseWALIndex) WALForTimestamp(timestamp int64) *DatabaseWAL {
// 	w.mutext.Lock()
// 	defer w.mutext.Unlock()

// 	if wal, ok := w.walVersions[timestamp]; ok {
// 		return wal
// 	}

// 	wal := NewDatabaseWAL(
// 		w.config,
// 		w.connectionManager,
// 		w.DatabaseId,
// 		w.BranchId,
// 		timestamp,
// 	)

// 	w.walVersions[timestamp] = wal

// 	return wal
// }
