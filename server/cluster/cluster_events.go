package cluster

import (
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type EventMessage struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

type EventChannel struct {
	Messages chan *EventMessage
}

// Broadcast a message to all of the nodes in the cluster.
func (c *Cluster) Broadcast(key string, value any) error {
	nodeIdentifiers := c.OtherNodes()

	var errors []error

	if len(nodeIdentifiers) == 0 {
		return nil
	}

	wg := sync.WaitGroup{}

	var mu sync.Mutex

	for _, nodeIdentifier := range nodeIdentifiers {
		wg.Add(1)

		go func(nodeIdentifier *NodeIdentifier) {
			defer wg.Done()

			attempts := 1
			maxAttempts := 1

			for attempts <= maxAttempts {
				if c.node.Context().Err() != nil {
					return
				}

				err := c.node.SendEvent(nodeIdentifier, NodeEvent{
					Key:   key,
					Value: value,
				})

				if err != nil {
					attempts++

					if attempts > maxAttempts {
						mu.Lock()
						errors = append(errors, err)
						mu.Unlock()
						break
					}

					time.Sleep(time.Duration(500*attempts) * time.Millisecond)
				} else {
					return
				}
			}
		}(nodeIdentifier)
	}

	wg.Wait()

	if len(errors) > 0 {
		return fmt.Errorf("failed to send event to some nodes: %v", errors)
	}

	return nil
}

// Receive an event to be passed to subscription channels.
func (c *Cluster) ReceiveEvent(message *EventMessage) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if _, ok := c.channels[message.Key]; ok {
		select {
		case <-c.node.Context().Done():
			slog.Debug("Context is cancelled, skipping event processing")
		case c.eventsChannel <- message:
			// message sent successfully
		}
	}
}

func (c *Cluster) SendEvent(nodeIdentifier *NodeIdentifier, key string, value any) error {
	var err error
	attempts := 1
	maxAttempts := 1

	for attempts <= maxAttempts {
		if c.Node().Context().Err() != nil {
			return nil
		}

		err = c.Node().SendEvent(nodeIdentifier, NodeEvent{
			Key:   key,
			Value: value,
		})

		if err != nil {
			attempts++

			if attempts > maxAttempts {
				slog.Debug("Failed to send event", "key", key, "node", nodeIdentifier.Address, "attempts", maxAttempts)
				break
			}

			time.Sleep(time.Duration(500*attempts) * time.Millisecond)
		} else {
			return nil
		}
	}

	if err != nil {
		return fmt.Errorf("failed to send event to some nodes: %v", err)
	}

	return nil
}

// Subscribe to a message from the cluster.
func (c *Cluster) Subscribe(key string, f func(message *EventMessage)) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if _, ok := c.channels[key]; !ok {
		c.channels[key] = []func(message *EventMessage){}
	}

	c.channels[key] = append(c.channels[key], f)
}

func (c *Cluster) runEventLoop() {

	go func() {
		defer close(c.eventsChannel)

		for {
			select {
			case <-c.Node().Context().Done():
				// close(c.eventsChannel)
				// c.channels = make(map[string][]func(message *EventMessage))
				return
			case message := <-c.eventsChannel:
				if handlers, ok := c.channels[message.Key]; ok {
					for _, handler := range handlers {
						handler(message)
					}
				}
			default:
				// No message received, continue the loop
				time.Sleep(100 * time.Millisecond) // Sleep to prevent busy waiting
			}
		}
	}()
}
