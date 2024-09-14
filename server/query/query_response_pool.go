package query

import "sync"

var staticQueryResponsePool *QueryResponsePool

type QueryResponsePool struct {
	Pool *sync.Pool
}

func ResponsePool() *QueryResponsePool {
	if staticQueryResponsePool == nil {
		staticQueryResponsePool = &QueryResponsePool{
			Pool: &sync.Pool{
				New: func() interface{} {
					return &QueryResponse{}
				},
			},
		}
	}

	return staticQueryResponsePool
}

func (qrp *QueryResponsePool) Get() *QueryResponse {
	response := qrp.Pool.Get().(*QueryResponse)
	response.Reset()

	return response
}

func (qrp *QueryResponsePool) Put(response *QueryResponse) {
	qrp.Pool.Put(response)
}
