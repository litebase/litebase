package cluster

type NodeIdentifier struct {
	Address string `json:"address"`
	Port    string `json:"port"`
}

func NewNodeIdentifier(address string, port string) *NodeIdentifier {
	return &NodeIdentifier{
		Address: address,
		Port:    port,
	}
}

/*
Returns a string representation of the NodeIdentifier.
*/
func (ni *NodeIdentifier) String() string {
	return ni.Address + ":" + ni.Port
}
