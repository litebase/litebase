package cluster

import "time"

type NodeIdentifier struct {
	Address  string    `json:"address"`
	ID       string    `json:"id,omitempty"`
	UpdateAt time.Time `json:"update_at"`
}

func NewNodeIdentifier(address string, id string, updateAt time.Time) *NodeIdentifier {
	return &NodeIdentifier{
		Address:  address,
		ID:       id,
		UpdateAt: updateAt,
	}
}
