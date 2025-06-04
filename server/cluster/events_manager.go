package cluster

import (
	"log/slog"
	"strconv"
)

type EventsManager struct {
	cluster *Cluster
	hooks   []func(key string, value string)
}

// Return the static instance of the eventsManager
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

// Initialize the events manager
func (em *EventsManager) Init() {
	em.cluster.Subscribe("activate_signature", func(message *EventMessage) {
		ActivateSignatureHandler(em.cluster.Config, message.Value)
	})

	em.cluster.Subscribe("cluster:join", func(message *EventMessage) {
		data := message.Value.(map[string]any)

		if _, ok := message.Value.(map[string]any); !ok {
			slog.Error("Cluster join event missing data")
			return
		}

		if _, ok := data["address"]; !ok {
			slog.Error("Cluster join event missing address")
			return
		}

		if _, ok := data["ID"]; !ok {
			slog.Error("Cluster join event missing ID")
			return
		}

		ID, err := strconv.ParseUint(data["ID"].(string), 10, 64)

		if err != nil {
			slog.Error("Failed to parse ID:", "error", err)
			return
		}

		em.cluster.AddMember(ID, data["address"].(string))
	})

	em.cluster.Subscribe("cluster:leave", func(message *EventMessage) {
		data := message.Value.(map[string]any)

		if _, ok := message.Value.(map[string]any); !ok {
			slog.Error("Cluster leave event missing data")
			return
		}

		if _, ok := data["address"]; !ok {
			slog.Error("Cluster leave event missing address")
			return
		}

		if address, ok := data["address"].(string); ok {
			em.cluster.RemoveMember(address, false)
		}
	})

	em.cluster.Subscribe("next_signature", func(message *EventMessage) {
		NextSignatureHandler(em.cluster.Config, message.Value)
	})
}
