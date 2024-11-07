package cluster

import (
	"crypto/sha256"
	"errors"
	"litebase/server/cluster/messages"
	"log"
	"time"
)

func (n *Node) HandleMessage(message messages.NodeMessage) (messages.NodeMessage, error) {
	var responseMessage interface{}

	switch message := message.Data.(type) {
	case messages.HeartbeatMessage:
		responseMessage = n.handleHeartbeatMessage(message)
	case messages.NodeConnectionMessage:
		responseMessage = messages.NodeConnectionMessage{
			ID: message.ID,
		}
	case messages.ReplicationGroupWriteCommitMessage:
		responseMessage = n.handleReplicationGroupWriteCommitMessage(message)
	case messages.ReplicationGroupWritePrepareMessage:
		responseMessage = n.handleReplicationGroupPrepareMessage(message)
	case messages.QueryMessage:
		responseMessage = n.handleQueryMessage(message)
	case messages.ReplicationGroupWriteMessage:
		responseMessage = n.handleReplicationGroupWriteMessage(message)
	default:
		err := n.handleBroadcastMessage(message)

		if err != nil {
			responseMessage = messages.ErrorMessage{
				Message: err.Error(),
			}
		} else {
			responseMessage = messages.NodeMessage{}
		}
	}

	return messages.NodeMessage{
		Data: responseMessage,
	}, nil
}

func (n *Node) handleBroadcastMessage(message interface{}) error {
	switch message := message.(type) {
	case messages.ReplicationGroupAssignmentMessage:
		if err := n.ReplicationGroupManager.HandledReplcationGroupAssignmentMessage(message); err != nil {
			return err
		}
	case messages.WALReplicationWriteMessage:
		if err := n.handleWALReplicationWriteMessage(message); err != nil {
			return err
		}
	case messages.WALReplicationTruncateMessage:
		if err := n.handleWALReplicationTruncateMessage(message); err != nil {
			return err
		}
	}

	return nil
}

// Handle a heartbeat message from a primary or replica node.
func (n *Node) handleHeartbeatMessage(message messages.HeartbeatMessage) interface{} {
	var responseMessage interface{}

	if n.IsPrimary() {
		isPrimary := n.VerifyPrimaryStatus()

		if !isPrimary {
			responseMessage = messages.ErrorMessage{
				Message: "Node is not primary",
			}
		} else {
			// responseMessage = messages.ErrorMessage{
			// 	Message: "Node is the primary",
			// }
		}
	} else {
		if message.Time > n.PrimaryHeartbeat.Unix() {
			n.PrimaryHeartbeat = time.Unix(message.Time, 0)
		}
	}

	return responseMessage
}

// Handle a query message from a replica node.
func (n *Node) handleQueryMessage(message messages.QueryMessage) interface{} {
	query, err := n.queryBuilder.Build(
		message.AccessKeyId,
		message.DatabaseHash,
		message.DatabaseId,
		message.BranchId,
		message.Statement,
		message.Parameters,
		message.ID,
	)

	if err != nil {
		log.Println("Failed to build query: ", err)

		return messages.ErrorMessage{
			Message: err.Error(),
		}
	}

	var response NodeQueryResponse

	err = query.Resolve(response)

	if err != nil {
		log.Println("Failed to process query message: ", err)
		return messages.ErrorMessage{
			Message: err.Error(),
		}
	}

	return messages.QueryMessageResponse{
		Changes:         response.Changes(),
		Columns:         response.Columns(),
		ID:              message.ID,
		LastInsertRowID: response.LastInsertRowId(),
		Latency:         response.Latency(),
		RowCount:        response.RowCount(),
		Rows:            response.Rows(),
	}
}

func (n *Node) handleReplicationGroupWriteCommitMessage(message messages.ReplicationGroupWriteCommitMessage) interface{} {
	replicationGroup, err := n.ReplicationGroupManager.FindForAddresses(message.Addresses)

	if err != nil {
		log.Println("Failed to find replication group for addresses: ", err)
		return messages.ErrorMessage{
			Message: err.Error(),
		}
	}

	err = replicationGroup.AknowledgeCommit(message)

	if err != nil {
		log.Println("Failed to acknowledge commit to replication group: ", err)

		return messages.ErrorMessage{
			Message: err.Error(),
		}
	}

	return messages.ReplicationGroupWriteCommitResponse{
		Key: message.Key,
	}
}

func (n *Node) handleReplicationGroupWriteMessage(message messages.ReplicationGroupWriteMessage) interface{} {
	replicationGroup, err := n.ReplicationGroupManager.FindForAddresses(message.Addresses)

	if err != nil {
		log.Println("Failed to find replication group for addresses: ", err)
		return messages.ErrorMessage{
			Message: err.Error(),
		}
	}

	err = replicationGroup.AknowledgeWrite(message)

	if err != nil {
		log.Println("Failed to acknowledge write to replication group: ", err)

		return messages.ErrorMessage{
			Message: err.Error(),
		}
	}

	return messages.ReplicationGroupWriteResponse{
		Key: message.Key,
	}
}

func (n *Node) handleReplicationGroupPrepareMessage(message messages.ReplicationGroupWritePrepareMessage) interface{} {
	resplicationGroup, err := n.ReplicationGroupManager.FindForAddresses(message.Addresses)

	if err != nil {
		log.Println("Failed to find replication group for addresses: ", err)
		return messages.ErrorMessage{
			Message: err.Error(),
		}
	}

	resplicationGroup.AknowledgePrepare(message)

	return messages.ReplicationGroupWritePrepareResponse{
		Key: message.Key,
	}
}

func (n *Node) handleWALReplicationWriteMessage(message messages.WALReplicationWriteMessage) error {
	// Verify the integrity of the WAL data
	sha256Hash := sha256.Sum256(message.Data)

	if sha256Hash != message.Sha256 {
		log.Println("Failed to verify WAL data integrity")
		return errors.New("failed to verify WAL data integrity")
	}

	err := n.walSynchronizer.WriteAt(
		message.DatabaseId,
		message.BranchId,
		message.Data,
		message.Offset,
		message.Sequence,
		message.Timestamp,
	)

	if err != nil {
		log.Println("Failed to sync WAL data: ", err)
		return err
	}

	return nil
}

func (n *Node) handleWALReplicationTruncateMessage(message messages.WALReplicationTruncateMessage) error {
	err := n.walSynchronizer.Truncate(
		message.DatabaseId,
		message.BranchId,
		message.Size,
		message.Sequence,
		message.Timestamp,
	)

	if err != nil {
		log.Println("Failed to sync WAL data truncation: ", err)
		return err
	}

	return nil
}
