package counter

import (
	"fmt"
	"time"
)

var queryCounts = map[int64]map[string]map[int64]*QueryCount{}

const Key = "query_counts"

func Add(
	timestamp int64,
	databaseId string,
	branchId string,
) *QueryCount {
	newlyCreated := false
	key := GetKey(databaseId, branchId)

	if _, ok := queryCounts[timestamp]; !ok {
		queryCounts[timestamp] = map[string]map[int64]*QueryCount{}
		newlyCreated = true
	}

	if _, ok := queryCounts[timestamp][key]; !ok || newlyCreated {
		queryCounts[timestamp][key] = map[int64]*QueryCount{}
	}

	if _, ok := queryCounts[timestamp][key][timestamp]; !ok {
		queryCounts[timestamp][key][timestamp] = &QueryCount{
			Key:       key,
			Timestamp: timestamp,
			Count:     0,
		}
	}

	return queryCounts[timestamp][key][timestamp]
}

func Clear() {
	queryCounts = map[int64]map[string]map[int64]*QueryCount{}
}

func Get(timestamp int64) map[string]map[int64]*QueryCount {
	return queryCounts[timestamp]
}

func Increment(databaseId string, branchId string) {
	counter := Add(time.Now().Round(time.Minute).Unix(), databaseId, branchId)

	if counter == nil {
		return
	}

	counter.Increment()
}

// Return the key for the query count.
func GetKey(databaseId string, branchId string) string {
	return fmt.Sprintf("%s:%s", databaseId, branchId)
}
