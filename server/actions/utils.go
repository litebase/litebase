package actions

import (
	"litebase/server/counter"
	"time"
)

func activeDatabases(minutes int) []string {
	timestamp := time.Now().Add(-time.Minute * time.Duration(minutes)).Round(time.Minute).Unix()
	counterMap := counter.Get(timestamp)

	if counterMap == nil {
		return []string{}
	}

	keys := make([]string, 0, len(counterMap))

	for k := range counterMap {
		keys = append(keys, k)
	}

	return keys
}
