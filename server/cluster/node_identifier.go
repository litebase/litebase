package cluster

type NodeIdentifier struct {
	Address string `json:"address"`
	ID      uint64 `json:"id,omitempty"`
}

func NewNodeIdentifier(address string, id uint64) *NodeIdentifier {
	return &NodeIdentifier{
		Address: address,
		ID:      id,
	}
}
