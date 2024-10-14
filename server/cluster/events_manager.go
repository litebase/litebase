package cluster

import (
	"litebase/server/storage"
)

type EventsManager struct {
	cluster *Cluster
	hooks   []func(key string, value string)
}

/*
Return the static instance of the eventsManager
*/
func (c *Cluster) EventsManager() *EventsManager {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.eventsManager == nil {
		c.eventsManager = &EventsManager{
			cluster: c,
		}
	}

	return c.eventsManager
}

func (em *EventsManager) Hook() func(key string, value string) {
	hook := func(key string, value string) {
		em.cluster.Broadcast(key, value)
	}

	em.hooks = append(em.hooks, hook)

	return hook
}

/*
Initialize the events manager
*/
func (em *EventsManager) Init() {
	em.cluster.Subscribe("activate_signature", func(message *EventMessage) {
		ActivateSignatureHandler(message.Value)
	})

	em.cluster.Subscribe("cluster:join", func(message *EventMessage) {
		data := message.Value.(map[string]interface{})

		em.cluster.AddMember(data["group"].(string), data["address"].(string))

		// Clear disributed file system cache
		storage.ClearFSFiles()
	})

	em.cluster.Subscribe("cluster:leave", func(message *EventMessage) {
		data := message.Value.(map[string]interface{})

		em.cluster.RemoveMember(data["address"].(string))

		// Clear disributed file system cache
		storage.ClearFSFiles()
	})

	em.cluster.Subscribe("next_signature", func(message *EventMessage) {
		NextSignatureHandler(message.Value)
	})
}
