package database

import (
	"fmt"
	"time"
)

const (
	CounterKey = "query_counter"
)

var QueryCounts = make(map[int64]map[string]map[int64]*QueryCount)

type Counter struct{}

// Add the datbase to the query counter.
func AddQueryCount(timestamp int64, databaseId, branchId string) *QueryCount {
	newlyCreated := false
	key := Key(databaseId, branchId)

	if _, ok := QueryCounts[timestamp]; !ok {
		QueryCounts[timestamp] = make(map[string]map[int64]*QueryCount)
		newlyCreated = true
	}

	if _, ok := QueryCounts[timestamp][key]; !ok || newlyCreated {
		QueryCounts[timestamp][key] = make(map[int64]*QueryCount)
	}

	if _, ok := QueryCounts[timestamp][key][timestamp]; !ok {
		QueryCounts[timestamp][key][timestamp] = NewQueryCount(key, timestamp)
	}

	return QueryCounts[timestamp][key][timestamp]
}

// Clear the query counts.
func ClearQueryCounters() {
	QueryCounts = make(map[int64]map[string]map[int64]*QueryCount)
}

// Retrieve a map of counts for a given timestamp.
func GetQueryCount(timestamp int64, databaseId, branchId string) map[string]map[int64]*QueryCount {
	return QueryCounts[timestamp]
}

// Return the key for a database by branch.
func GetCounterKey(databaseId, branchId string) string {
	return fmt.Sprintf("%s:%s", databaseId, branchId)
}

// Increment a database's query counter.
func IncrementQueryCount(databaseId, branchId string, t time.Time) {
	timeToStartOfMinute := t.Round(time.Minute)
	timestamp := timeToStartOfMinute.Unix()
	counter := AddQueryCount(timestamp, databaseId, branchId)
	counter.Increment()
}

func Key(databaseId, branchId string) string {
	return fmt.Sprintf("%s:%s", databaseId, branchId)
}

func PurgeTimestamps(minutes int) {
	t := time.Now().UTC().Add(-time.Minute - time.Minute*time.Duration(minutes))
	timestamp := t.Round(time.Minute).Unix()

	for k := range QueryCounts {
		if k <= timestamp {
			delete(QueryCounts, k)
		}
	}
}

// Calculate the average requests per second for a database that have occurred
// in the last 3 minutes. The average is calculated by dividing the total
// number of requests by the number of minutes. The counts for each minute
// are weighted by the number of minutes since the last request.
func RequestsPerSecond(databaseId, branchId string) int {
	timestamps := []int64{
		time.Now().UTC().Add(-2 * time.Minute).Round(time.Minute).Unix(),
		time.Now().UTC().Add(-1 * time.Minute).Round(time.Minute).Unix(),
		time.Now().UTC().Round(time.Minute).Unix(),
	}

	counts := []int{0, 0, 0}

	for i, timestamp := range timestamps {
		if _, ok := QueryCounts[timestamp]; ok {
			key := Key(databaseId, branchId)

			if _, ok := QueryCounts[timestamp][key]; ok {
				counts[i] = QueryCounts[timestamp][key][timestamp].Count
			}
		}
	}

	weights := []float64{}

	if counts[0] == 0 {
		weights = append(weights, 0)
	} else {
		weights = append(weights, 0.05)
	}

	if counts[1] == 0 {
		weights = append(weights, 0)
	} else {
		weights = append(weights, 0.25)
	}

	weights = append(weights, 1-weights[0]-weights[1])

	average := weightedAverage(counts, weights) / 60

	return int(average)
}

func weightedAverage(nums []int, weights []float64) float64 {
	sum := float64(0)
	weightSum := float64(0)
	for i, w := range weights {
		sum = sum + float64(nums[i])*w
		weightSum = weightSum + w
	}

	return float64(sum) / float64(weightSum)
}
