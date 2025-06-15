package cluster

type NodeQueryResponsePool interface {
	Get() NodeQueryResponse
	Put(NodeQueryResponse)
}
