package cluster

import (
	"encoding/gob"
	"litebase/server/cluster/messages"
)

func registerNodeMessages() {
	gob.Register(messages.NodeMessage{})
	gob.Register(messages.ErrorMessage{})
	gob.Register(messages.HeartbeatMessage{})
	gob.Register(messages.NodeConnectionMessage{})
	gob.Register(messages.QueryMessage{})
	gob.Register(messages.QueryMessageResponse{})
	gob.Register(messages.ReplicationGroupAssignment{})
	gob.Register(messages.ReplicationGroupAssignmentMessage{})
	gob.Register(messages.ReplicationGroupWriteMessage{})
	gob.Register(messages.ReplicationGroupWriteResponse{})
	gob.Register(messages.ReplicationGroupWritePrepareMessage{})
	gob.Register(messages.ReplicationGroupWritePrepareResponse{})
	gob.Register(messages.ReplicationGroupWriteCommitMessage{})
	gob.Register(messages.ReplicationGroupWriteCommitResponse{})

	gob.Register(messages.WALReplicationWriteMessage{})
	gob.Register(messages.WALReplicationTruncateMessage{})
}
