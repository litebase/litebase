package cluster

type ErrorNoPrimaryAvailable struct{}

func (e ErrorNoPrimaryAvailable) Error() string {
	return "No primary key found"
}

type ErrorRecursiveProxyRequest struct{}

func (e ErrorRecursiveProxyRequest) Error() string {
	return "primary address is the same as the current node address"
}
