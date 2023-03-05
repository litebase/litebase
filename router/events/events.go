package events

import (
	"litebasedb/router/node"
	"log"
)

type EventChannel struct {
	Messages chan *node.NodeEvent
}

var channels map[string]EventChannel

// Broadcast a message to all of the nodes in the cluster.
func Broadcast(key string, value interface{}) {
	nodes := node.OtherNodes()
	if len(nodes) > 0 {
		log.Println("Broadcasting to nodes: ", nodes)
	}

	for _, n := range nodes {
		go node.SendEvent(n, node.NodeEvent{
			Key:   key,
			Value: value,
		})
	}
}

func ReceiveEvent(message *node.NodeEvent) {
	if channels == nil {
		channels = make(map[string]EventChannel)
	}

	if _, ok := channels[message.Key]; !ok {
		channels[message.Key] = EventChannel{
			Messages: make(chan *node.NodeEvent),
		}
	}

	channels[message.Key].Messages <- message
}

// Subscribe to a message from the cluster.
func Subscribe(key string, f func(message *node.NodeEvent)) {
	if channels == nil {
		channels = make(map[string]EventChannel)
	}

	if _, ok := channels[key]; !ok {
		channels[key] = EventChannel{
			Messages: make(chan *node.NodeEvent),
		}
	}

	go func() {
		for {
			message := <-channels[key].Messages

			f(message)
		}
	}()
}
