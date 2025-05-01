package http

import (
	"bytes"
	"context"
	"encoding/gob"
	"io"
	"log"
	"net/http"
	"sync"

	"github.com/litebase/litebase/server/cluster/messages"
)

var clusterConnectionBufferPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

func ClusterConnectionController(request *Request) Response {
	return Response{
		StatusCode: 200,
		Stream: func(w http.ResponseWriter) {
			w.Header().Set("Connection", "close")
			w.Header().Set("Content-Type", "application/gob")
			w.Header().Set("Transfer-Encoding", "chunked")

			defer request.BaseRequest.Body.Close()

			ctx, cancel := context.WithCancel(request.BaseRequest.Context())

			go handleClusterConnectionStream(request, cancel, request.BaseRequest.Body, w)

			<-ctx.Done()

			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		},
	}
}

// Read a stream of messages from the client and write a stream of responses back
// to the client.
func handleClusterConnectionStream(
	request *Request,
	cancel context.CancelFunc,
	reader io.ReadCloser,
	w http.ResponseWriter,
) {
	scanBuffer := clusterConnectionBufferPool.Get().(*bytes.Buffer)
	defer clusterConnectionBufferPool.Put(scanBuffer)

	var decodedMessage messages.NodeMessage

	for {
		decoder := gob.NewDecoder(reader)

		err := decoder.Decode(&decodedMessage)

		if err != nil {
			if err != io.ErrUnexpectedEOF {
				log.Println(err)
			}

			break
		}

		nodeResponseMessage, err := request.cluster.Node().HandleMessage(decodedMessage)

		if err != nil {
			log.Println(err)
			continue
		}

		writeNodeMessageResponse(w, nodeResponseMessage)
	}

	cancel()
}

// Write a response to the client.
func writeNodeMessageResponse(
	w http.ResponseWriter,
	nodeResponseMessage interface{},
) {
	encoder := gob.NewEncoder(w)

	err := encoder.Encode(nodeResponseMessage)

	if err != nil {
		log.Println(err)
		return
	}

	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}
