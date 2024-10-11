package cluster

type NodeIdentifier struct {
	Address string `json:"address"`
	Port    string `json:"port"`
}

/*
Returns a string representation of the NodeIdentifier.
*/
func (ni *NodeIdentifier) String() string {
	return ni.Address + ":" + ni.Port
}
