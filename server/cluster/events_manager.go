package cluster

import (
	"litebase/server/storage"
)

type EventsManagerInstance struct {
	hooks []func(key string, value string)
}

var staticEventsManager *EventsManagerInstance

func EventsManager() *EventsManagerInstance {
	if staticEventsManager == nil {
		staticEventsManager = &EventsManagerInstance{}
	}

	return staticEventsManager
}

func (s *EventsManagerInstance) Hook() func(key string, value string) {
	hook := func(key string, value string) {
		Node().Broadcast(key, value)
	}

	s.hooks = append(s.hooks, hook)

	return hook
}

func (s *EventsManagerInstance) Init() {
	Subscribe("activate_signature", func(message EventMessage) {
		ActivateSignatureHandler(message.Value)
	})

	Subscribe("cluster:join", func(message EventMessage) {
		data := message.Value.(map[string]interface{})

		Get().AddMember(data["group"].(string), data["address"].(string))

		// Clear disributed file system cache
		storage.ClearFSFiles()
	})

	Subscribe("cluster:leave", func(message EventMessage) {
		data := message.Value.(map[string]interface{})

		Get().RemoveMember(data["address"].(string))

		// Clear disributed file system cache
		storage.ClearFSFiles()
	})

	Subscribe("next_signature", func(message EventMessage) {
		NextSignatureHandler(message.Value)
	})
}
