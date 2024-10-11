package cluster

type NodeEvent struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}
