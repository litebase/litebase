package cluster

type NodeIdentifier struct {
	Address string `json:"address"`
	ID      string `json:"id,omitempty"`
}

func NewNodeIdentifier(address string, id string) *NodeIdentifier {
	return &NodeIdentifier{
		Address: address,
		ID:      id,
	}
}
