package cluster

import (
	"encoding/gob"
	"litebase/server/cluster/messages"
)

func registerNodeMessages() {
	gob.Register(messages.ErrorMessage{})
	gob.Register(messages.NodeConnectionMessage{})
	gob.Register(messages.QueryMessage{})
	gob.Register(messages.QueryMessageResponse{})
	gob.Register(messages.WALReplicationWriteMessage{})
	gob.Register(messages.WALReplicationTruncateMessage{})
}
