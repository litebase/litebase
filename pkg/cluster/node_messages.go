package cluster

import (
	"encoding/gob"

	"github.com/litebase/litebase/pkg/cluster/messages"
)

func registerNodeMessages() {
	gob.Register(messages.NodeMessage{})
	gob.Register(messages.RangeReplicationTruncateMessage{})
	gob.Register(messages.RangeReplicationWriteMessage{})
	gob.Register(messages.ErrorMessage{})
	gob.Register(messages.HeartbeatMessage{})
	gob.Register(messages.HeartbeatResponseMessage{})
	gob.Register(messages.NodeConnectionMessage{})

	// Database messages
	gob.Register(messages.DatabaseBranchSettingsUpdated{})

	// Page Logger messages
	gob.Register(messages.PageLoggerVersionUsageRequest{})
	gob.Register(messages.PageLoggerVersionUsageResponse{})

	gob.Register(messages.QueryMessage{})
	gob.Register(messages.QueryMessageResponse{})

	// WAL messages
	gob.Register(messages.WALIndexHeaderMessage{})
	gob.Register(messages.WALIndexTimestampMessage{})
	gob.Register(messages.WALReplicationWriteMessage{})
	gob.Register(messages.WALVersionUsageRequest{})
	gob.Register(messages.WALVersionUsageResponse{})
}
