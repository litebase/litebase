package node

import (
	"log"
	"sync"
)

type EventMessage struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

type EventChannel struct {
	Messages chan EventMessage
}

var channels map[string]EventChannel
var channelsMutex sync.RWMutex

// Broadcast a message to all of the nodes in the cluster.
func (n *NodeInstance) Broadcast(key string, value interface{}) error {
	nodes := n.OtherNodes()

	wg := sync.WaitGroup{}

	for _, node := range nodes {
		wg.Add(1)

		go func(node *NodeIdentifier) {
			defer wg.Done()
			err := n.SendEvent(node, NodeEvent{
				Key:   key,
				Value: value,
			})

			if err != nil {
				log.Println("Error sending event to node ", node.Address, err)
			}
		}(node)
	}

	wg.Wait()

	return nil
}

func ReceiveEvent(message *EventMessage) {
	if channels == nil {
		channels = make(map[string]EventChannel)
	}

	if _, ok := channels[message.Key]; !ok {
		channels[message.Key] = EventChannel{
			Messages: make(chan EventMessage),
		}
	}

	channels[message.Key].Messages <- *message
}

// Subscribe to a message from the cluster.
func Subscribe(key string, f func(message EventMessage)) {
	channelsMutex.Lock()
	defer channelsMutex.Unlock()

	if channels == nil {
		channels = make(map[string]EventChannel)
	}

	if _, ok := channels[key]; !ok {
		channels[key] = EventChannel{
			Messages: make(chan EventMessage),
		}
	}

	go func() {
		for {
			select {
			case <-Node().Context().Done():
				return
			case message := <-channels[key].Messages:

				f(message)
			}
		}
	}()
}
