package logs

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math"
	"sort"
	"strconv"

	"github.com/litebase/litebase/internal/utils"
)

// A query metric is a snapshot of the query performance at a given time. These
// statistics can be stored on disk and only take 64 bytes of space.
type QueryMetric struct {
	Checksum     uint64
	Count        uint32 `json:"count"`
	latencies    []float64
	latencyIndex int
	LatencyAvg   float64 `json:"latency_avg"`
	LatencyMin   float64 `json:"latency_min"`
	LatencyMax   float64 `json:"latency_max"`
	LatencyP50   float64 `json:"latency_p50"`
	LatencyP90   float64 `json:"latency_p90"`
	LatencyP99   float64 `json:"latency_p99"`
	Timestamp    uint32  `json:"timestamp"`
}

const LatencyBufferSize = 128

func NewQueryMetric(timestamp int64, checksum uint64) *QueryMetric {
	uint32Timestamp, err := utils.SafeInt64ToUint32(timestamp)

	if err != nil {
		return nil
	}

	return &QueryMetric{
		Checksum:     checksum,
		Count:        1,
		latencyIndex: 0,
		latencies:    make([]float64, LatencyBufferSize),
		Timestamp:    uint32Timestamp,
	}
}

func (q *QueryMetric) AddLatency(latency float64) {
	if q.latencyIndex >= len(q.latencies) {
		// Grow the slice
		q.latencies = append(q.latencies, make([]float64, LatencyBufferSize)...)
	}

	q.latencies[q.latencyIndex] = latency
	q.latencyIndex++
}

func (q *QueryMetric) Bytes(buf *bytes.Buffer) ([]byte, error) {
	var err error

	q.Count, err = utils.SafeIntToUint32(len(q.latencies))

	if err != nil {
		return nil, err
	}

	q.calculateLatencies()

	err = binary.Write(buf, binary.LittleEndian, q.Checksum)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, q.Count)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, math.Float64bits(q.LatencyAvg))
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, math.Float64bits(q.LatencyMin))
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, math.Float64bits(q.LatencyMax))
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, math.Float64bits(q.LatencyP50))
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, math.Float64bits(q.LatencyP90))
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, math.Float64bits(q.LatencyP99))
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, q.Timestamp)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (q *QueryMetric) calculateLatencies() {
	sort.Float64s(q.latencies)

	q.LatencyAvg = 0
	q.LatencyMin = 0
	q.LatencyMax = 0

	for _, latency := range q.latencies {
		q.LatencyAvg += latency

		if q.LatencyMin == 0 || latency < q.LatencyMin {
			q.LatencyMin = latency
		}

		if q.LatencyMax == 0 || latency > q.LatencyMax {
			q.LatencyMax = latency
		}
	}

	q.LatencyAvg = q.LatencyAvg / float64(len(q.latencies))

	q.calculatePercentiles()
}

func (q *QueryMetric) calculatePercentiles() {
	q.LatencyP50 = q.calculatePercentile(50)
	q.LatencyP90 = q.calculatePercentile(95)
	q.LatencyP99 = q.calculatePercentile(99)
}

func (q *QueryMetric) calculatePercentile(percentile float64) float64 {
	if len(q.latencies) == 0 {
		return 0
	}

	// Calculate the index of the percentile
	index := int(float64(len(q.latencies)) * (percentile / 100))

	// Return the value at the index
	return q.latencies[index]
}

func (q QueryMetric) Combine(other QueryMetric) QueryMetric {
	q.Count += other.Count
	q.LatencyAvg = (q.LatencyAvg + other.LatencyAvg) / 2
	q.LatencyMin = math.Min(q.LatencyMin, other.LatencyMin)
	q.LatencyMax = math.Max(q.LatencyMax, other.LatencyMax)
	q.LatencyP50 = (q.LatencyP50 + other.LatencyP50) / 2
	q.LatencyP90 = (q.LatencyP90 + other.LatencyP90) / 2
	q.LatencyP99 = (q.LatencyP99 + other.LatencyP99) / 2

	return q
}

func (qm QueryMetric) MarshalJSON() ([]byte, error) {
	return json.Marshal([]interface{}{
		strconv.FormatUint(qm.Checksum, 16),
		qm.Count,
		qm.LatencyAvg,
		qm.LatencyMin,
		qm.LatencyMax,
		qm.LatencyP50,
		qm.LatencyP90,
		qm.LatencyP99,
		qm.Timestamp,
	})
}

func QueryMetricFromBytes(data []byte) (QueryMetric, error) {
	q := QueryMetric{}
	buf := bytes.NewReader(data)

	err := binary.Read(buf, binary.LittleEndian, &q.Checksum)
	if err != nil {
		return QueryMetric{}, err
	}

	err = binary.Read(buf, binary.LittleEndian, &q.Count)
	if err != nil {
		return QueryMetric{}, err
	}

	err = binary.Read(buf, binary.LittleEndian, &q.LatencyAvg)
	if err != nil {
		return QueryMetric{}, err
	}

	err = binary.Read(buf, binary.LittleEndian, &q.LatencyMin)
	if err != nil {
		return QueryMetric{}, err
	}

	err = binary.Read(buf, binary.LittleEndian, &q.LatencyMax)
	if err != nil {
		return QueryMetric{}, err
	}

	err = binary.Read(buf, binary.LittleEndian, &q.LatencyP50)
	if err != nil {
		return QueryMetric{}, err
	}

	err = binary.Read(buf, binary.LittleEndian, &q.LatencyP90)
	if err != nil {
		return QueryMetric{}, err
	}

	err = binary.Read(buf, binary.LittleEndian, &q.LatencyP99)
	if err != nil {
		return QueryMetric{}, err
	}

	err = binary.Read(buf, binary.LittleEndian, &q.Timestamp)
	if err != nil {
		return QueryMetric{}, err
	}

	return q, nil
}

func QueryMetricKeys() []string {
	return []string{
		"id",
		"count",
		"latency_avg",
		"latency_min",
		"latency_max",
		"latency_p50",
		"latency_p90",
		"latency_p99",
		"timestamp",
	}
}
