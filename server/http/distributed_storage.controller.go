package http

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"litebase/server/storage"
	"log"
	"net/http"
	"sync"
)

var storageStreamBufferPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 1024))
	},
}

/*
Handle storage requests from query nodes. This function reads a stream of
messages from the client and writes a stream of responses back to the client.
*/
func DistributedStorageController(request *Request) Response {
	return Response{
		StatusCode: 200,
		Stream: func(w http.ResponseWriter) {
			w.Header().Set("Connection", "close")
			w.Header().Set("Content-Type", "text/plain")
			w.Header().Set("Transfer-Encoding", "chunked")

			defer request.BaseRequest.Body.Close()

			ctx, cancel := context.WithCancel(context.Background())

			go handleStream(cancel, request.BaseRequest.Body, w)

			<-ctx.Done()
		},
	}
}

/*
Read a stream of messages from the client and write a stream of responses back
to the client.
*/
func handleStream(
	cancel context.CancelFunc,
	reader io.ReadCloser,
	w http.ResponseWriter,
) {
	scanBuffer := storageStreamBufferPool.Get().(*bytes.Buffer)
	defer storageStreamBufferPool.Put(scanBuffer)

	var dfsRequest storage.DistributedFileSystemRequest
	messageLengthBytes := make([]byte, 4)

	// Read the stream of messages from the client. Each message starts with a
	// 4-byte length prefix.
	for {
		_, err := reader.Read(messageLengthBytes)

		if err != nil {
			break
		}

		messageLength := int(binary.LittleEndian.Uint32(messageLengthBytes))

		scanBuffer.Reset()

		// Read the message in chunks
		bytesRead := 0

		for bytesRead < messageLength {
			chunkSize := 1024 // Define a chunk size

			if messageLength-bytesRead < chunkSize {
				chunkSize = messageLength - bytesRead
			}

			n, err := io.CopyN(scanBuffer, reader, int64(chunkSize))

			if err != nil {
				log.Println(err)
				break
			}

			bytesRead += int(n)
		}

		dfsRequest, err := storage.DecodeDistributedFileSystemRequest(dfsRequest, scanBuffer.Next(messageLength))

		if err != nil {
			log.Println(err)
			break
		}

		writeResponse(w, dfsRequest, messageLengthBytes)
	}

	cancel()
}

/*
Write a response to the client.
*/
func writeResponse(
	w http.ResponseWriter,
	dfsRequest storage.DistributedFileSystemRequest,
	responsePrefixBytes []byte,
) {
	var dfsResponse storage.DistributedFileSystemResponse

	// log.Println("Handling request:", dfsRequest.Path)
	dfsResponse = storage.HandleDistributedStorageRequest(
		dfsRequest,
		dfsResponse,
	)

	encoded := dfsResponse.Encode()

	binary.LittleEndian.PutUint32(responsePrefixBytes, uint32(len(encoded)))

	_, err := w.Write(responsePrefixBytes)

	if err != nil {
		log.Println(err)
		return
	}

	_, err = w.Write(encoded)

	if err != nil {
		log.Println(err)
		return
	}

	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}
