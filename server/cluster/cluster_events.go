package cluster

import (
	"sync"
)

type EventMessage struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

type EventChannel struct {
	Messages chan *EventMessage
}

/*
Broadcast a message to all of the nodes in the cluster.
*/
func (c *Cluster) Broadcast(key string, value interface{}) error {
	nodeIdentifiers := c.OtherNodes()

	wg := sync.WaitGroup{}

	for _, nodeIdentifier := range nodeIdentifiers {
		wg.Add(1)

		go func(nodeIdentifier *NodeIdentifier) {
			defer wg.Done()

			err := c.Node().SendEvent(nodeIdentifier, NodeEvent{
				Key:   key,
				Value: value,
			})

			if err != nil {
				// log.Printf("Error sending event to node %s from node %s: %s", nodeIdentifier.String(), c.Node().Address(), err)
			}
		}(nodeIdentifier)
	}

	wg.Wait()

	return nil
}

/*
Receive an event to be passed to subscription channels.
*/
func (c *Cluster) ReceiveEvent(message *EventMessage) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if _, ok := c.channels[message.Key]; ok {
		c.channels[message.Key].Messages <- message
	}
}

/*
Subscribe to a message from the cluster.
*/
func (c *Cluster) Subscribe(key string, f func(message *EventMessage)) {
	c.mutex.Lock()

	if _, ok := c.channels[key]; !ok {
		c.channels[key] = EventChannel{
			Messages: make(chan *EventMessage, 10),
		}
	}

	c.mutex.Unlock()

	go func(messagesChannel chan *EventMessage) {
		for {
			select {
			case <-c.Node().Context().Done():
				return
			case message := <-messagesChannel:
				f(message)
			}
		}
	}(c.channels[key].Messages)
}
