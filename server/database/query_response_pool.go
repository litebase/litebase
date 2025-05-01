package database

import (
	"sync"

	"github.com/litebase/litebase/server/cluster"
)

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

func (qrp *QueryResponsePool) Get() cluster.NodeQueryResponse {
	response := qrp.Pool.Get().(*QueryResponse)
	response.Reset()

	return response
}

func (qrp *QueryResponsePool) Put(response cluster.NodeQueryResponse) {
	qrp.Pool.Put(response)
}
