package cluster

import (
	"log/slog"
)

type EventsManager struct {
	cluster *Cluster
	hooks   []func(key string, value string)
}

type EventHandler func(message *EventMessage)

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
		err := em.cluster.Broadcast(key, value)

		if err != nil {
			slog.Debug("Failed to broadcast event", "key", key, "error", err)
		}
	}

	em.hooks = append(em.hooks, hook)

	return hook
}

// Initialize the events manager
func (em *EventsManager) Init() {
	em.cluster.Subscribe("access-key:purge", func(message *EventMessage) {
		if accessKeyID, ok := message.Value.(string); ok {
			err := em.cluster.Auth.AccessKeyManager.Purge(accessKeyID)

			if err != nil {
				slog.Error("Failed to purge access key", "error", err)
			}
		}
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

		ID, ok := data["ID"].(string)

		if !ok {
			slog.Error("Failed to parse ID:")
			return
		}

		err := em.cluster.AddMember(ID, data["address"].(string))

		if err != nil {
			slog.Error("Failed to add member to cluster", "error", err)
		}
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
			err := em.cluster.RemoveMember(address, false)

			if err != nil {
				slog.Error("Failed to remove member from cluster", "error", err)
			}
		}
	})

	em.cluster.Subscribe("database:delete", func(message *EventMessage) {
		// TODO: Implement database delete
	})

	em.cluster.Subscribe("database-settings:purge", func(message *EventMessage) {
		// TODO: Implement database settings purge
	})

	em.cluster.Subscribe("key:activate", func(message *EventMessage) {
		err := ActivateKeyHandler(em.cluster.Config, message.Value)

		if err != nil {
			slog.Error("Failed to activate key", "error", err)
		}
	})

	em.cluster.Subscribe("key:next", func(message *EventMessage) {
		NextKeyHandler(em.cluster.Config, message.Value)
	})

	em.cluster.Subscribe("user:purge", func(message *EventMessage) {
		if username, ok := message.Value.(string); ok {
			err := em.cluster.Auth.UserManager().Purge(username)

			if err != nil {
				slog.Error("Failed to purge user", "error", err)
			}
		}
	})
}
