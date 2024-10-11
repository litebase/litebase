package http

import (
	"bytes"
	"context"
	"encoding/gob"
	"io"
	"litebase/server/cluster"
	"log"
	"net/http"
)

func ClusterConnectionController(request *Request) Response {
	return Response{
		StatusCode: 200,
		Stream: func(w http.ResponseWriter) {
			w.Header().Set("Connection", "close")
			w.Header().Set("Content-Type", "application/gob")
			w.Header().Set("Transfer-Encoding", "chunked")

			defer request.BaseRequest.Body.Close()

			ctx, cancel := context.WithCancel(cluster.Node().Context())

			go handleClusterConnectionStream(cancel, request.BaseRequest.Body, w)

			<-ctx.Done()
		},
	}
}

/*
Read a stream of messages from the client and write a stream of responses back
to the client.
*/
func handleClusterConnectionStream(
	cancel context.CancelFunc,
	reader io.ReadCloser,
	w http.ResponseWriter,
) {
	scanBuffer := storageStreamBufferPool.Get().(*bytes.Buffer)
	defer storageStreamBufferPool.Put(scanBuffer)

	var nodeMessage cluster.NodeMessage

	for {
		decoder := gob.NewDecoder(reader)

		err := decoder.Decode(&nodeMessage)

		if err != nil {
			if err != io.ErrUnexpectedEOF {
				log.Println(err)
			}

			break
		}

		var nodeResponseMessage cluster.NodeMessage

		if cluster.Node().IsPrimary() {
			nodeResponseMessage, err = cluster.Node().Primary().HandleMessage(nodeMessage)
		} else {
			nodeResponseMessage, err = cluster.Node().Replica().HandleMessage(nodeMessage)
		}

		if err != nil {
			log.Println(err)
			continue
		}

		writeNodeMessageResponse(w, nodeResponseMessage)
	}

	cancel()
}

/*
Write a response to the client.
*/
func writeNodeMessageResponse(
	w http.ResponseWriter,
	nodeResponseMessage cluster.NodeMessage,
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
