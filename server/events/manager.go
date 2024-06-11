package events

import (
	"litebase/server/node"
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
		Broadcast(key, value)
	}

	s.hooks = append(s.hooks, hook)

	return hook
}

func (s *EventsManagerInstance) Init() {
	Subscribe("activate_signature", func(message *node.NodeEvent) {
		ActivateSignatureHandler(message.Value)
	})

	Subscribe("next_signature", func(message *node.NodeEvent) {
		NextSignatureHandler(message.Value)
	})
}
