package cluster

import (
	"log"
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
	nodes := c.Node().OtherNodes()

	wg := sync.WaitGroup{}

	for _, node := range nodes {
		wg.Add(1)

		go func(node *NodeIdentifier) {
			defer wg.Done()

			err := c.Node().SendEvent(node, NodeEvent{
				Key:   key,
				Value: value,
			})

			if err != nil {
				log.Printf("Error sending event to node %s from node %s: %s", node.String(), c.Node().Address(), err)
			}
		}(node)
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
			Messages: make(chan *EventMessage),
		}
	}

	c.mutex.Unlock()

	go func() {
		for {
			select {
			case <-c.Node().Context().Done():
				return
			case message := <-c.channels[key].Messages:

				f(message)
			}
		}
	}()
}
