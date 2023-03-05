package runtime

import (
	"litebasedb/internal/config"
	"litebasedb/runtime/event"
	"litebasedb/runtime/http"
)

func Handler(event *event.Event) *http.Response {
	configure(event)

	return http.Router().Dispatch(event)
}

func configure(event *event.Event) {
	if event.DatabaseUuid != "" {
		config.Set("database_uuid", event.DatabaseUuid)
	}

	if event.BranchUuid != "" {
		config.Set("branch_uuid", event.BranchUuid)
	}
}
