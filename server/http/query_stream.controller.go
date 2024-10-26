package http

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"litebase/server/auth"
	"litebase/server/database"
	"log"
	"net/http"
	"sync"
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 1024))
	},
}

// Define a sync.Pool for reusable Command structs
var inputPool = &sync.Pool{
	New: func() interface{} {
		return &database.QueryInput{}
	},
}

const QueryStreamFlushInterval = 0

type QueryStreamMessageType int

const (
	QueryStreamOpenConnection  QueryStreamMessageType = 0x01
	QueryStreamCloseConnection QueryStreamMessageType = 0x02
	QueryStreamError           QueryStreamMessageType = 0x03
	QueryStreamFrame           QueryStreamMessageType = 0x04
	QueryStreamFrameEntry      QueryStreamMessageType = 0x05
)

func QueryStreamController(request *Request) Response {
	databaseKey, err := database.GetDatabaseKey(
		request.cluster.Config,
		request.cluster.ObjectFS(),
		request.Subdomains()[0],
	)

	if err != nil {
		return BadRequestResponse(fmt.Errorf("a valid database is required to make this request"))
	}

	requestToken := request.RequestToken("Authorization")

	if !requestToken.Valid() {
		return BadRequestResponse(fmt.Errorf("a valid access key is required to make this request"))
	}

	accessKey := requestToken.AccessKey(databaseKey.DatabaseId)

	if accessKey.AccessKeyId == "" {
		return BadRequestResponse(fmt.Errorf("a valid access key is required to make this request"))
	}

	return Response{
		StatusCode: 200,
		Stream: func(w http.ResponseWriter) {
			w.Header().Set("Connection", "close")
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Transfer-Encoding", "chunked")

			defer request.BaseRequest.Body.Close()
			ctx, cancel := context.WithCancel(context.Background())

			readQueryStream(cancel, request, w, databaseKey, accessKey)

			<-ctx.Done()
		},
	}
}

func processInput(
	request *Request,
	databaseKey *database.DatabaseKey,
	accessKey *auth.AccessKey,
	input *database.QueryInput,
	response *database.QueryResponse,
) error {
	requestQuery := database.Get(
		request.cluster,
		request.databaseManager,
		databaseKey,
		accessKey,
		input,
	)

	defer database.Put(requestQuery)

	err := requestQuery.Resolve(response)

	if err != nil {
		return err
	}

	return nil
}

func readQueryStream(
	cancel context.CancelFunc,
	request *Request,
	w http.ResponseWriter,
	databaseKey *database.DatabaseKey,
	accessKey *auth.AccessKey,
) {
	defer cancel()

	scanBuffer := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(scanBuffer)

	streamMutex := &sync.Mutex{}

	messageHeaderBytes := make([]byte, 5)

	for {
		scanBuffer.Reset()

		_, err := request.BaseRequest.Body.Read(messageHeaderBytes)

		if err != nil {
			cancel()
			break
		}

		messageLength := int(binary.LittleEndian.Uint32(messageHeaderBytes[1:]))

		// Read the message in chunks
		bytesRead := 0

		for bytesRead < messageLength {
			chunkSize := 1024 // Define a chunk size

			if messageLength-bytesRead < chunkSize {
				chunkSize = messageLength - bytesRead
			}

			n, err := io.CopyN(scanBuffer, request.BaseRequest.Body, int64(chunkSize))

			if err != nil {
				log.Println(err)
				break
			}

			bytesRead += int(n)
		}

		// Convert the message type to an integer
		messageType := int(messageHeaderBytes[0])

		switch QueryStreamMessageType(messageType) {
		case QueryStreamOpenConnection:
			err := handleQueryStreamConnection(w, streamMutex, scanBuffer.Next(messageLength))

			if err != nil {
				log.Println(err)
				return
			}

			// continue
		case QueryStreamCloseConnection:
			log.Println("Closing connection")
			cancel()
			return
		case QueryStreamFrame:
			queryStreamFrameBuffer := bufferPool.Get().(*bytes.Buffer)
			queryStreamFrameBuffer.Reset()

			// Copy the message to the query input buffer
			queryStreamFrameBuffer.Write(scanBuffer.Next(messageLength))
			handleQueryStreamFrame(request, w, streamMutex, queryStreamFrameBuffer, databaseKey, accessKey)
			bufferPool.Put(queryStreamFrameBuffer)
		default:
			log.Println("Unknown message type", messageType)
			// return
		}
	}
}

func handleQueryStreamRequest(
	request *Request,
	databaseKey *database.DatabaseKey,
	accessKey *auth.AccessKey,
	queryData *bytes.Buffer,
) ([]byte, error) {
	responseBuffer := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(responseBuffer)
	responseBuffer.Reset()

	rowsBuffer := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(rowsBuffer)
	rowsBuffer.Reset()

	columnsBuffer := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(columnsBuffer)
	columnsBuffer.Reset()

	response := database.ResponsePool().Get()
	defer database.ResponsePool().Put(response)

	queryInput := database.QueryInputPool.Get().(*database.QueryInput)
	defer database.QueryInputPool.Put(queryInput)

	queryInput.Reset()

	err := queryInput.Decode(queryData)

	if err != nil {
		return nil, err
	}

	response.Reset()

	err = processInput(request, databaseKey, accessKey, queryInput, response)

	if err != nil {
		return nil, err
	}

	return response.Encode(responseBuffer, rowsBuffer, columnsBuffer)
}

func handleQueryStreamConnection(w http.ResponseWriter, streamMutex *sync.Mutex, read []byte) error {
	message := []byte("connected")
	data := bytes.NewBuffer(make([]byte, 0))
	data.WriteByte(uint8(QueryStreamOpenConnection))
	var messageLengthbytes [4]byte
	binary.LittleEndian.PutUint32(messageLengthbytes[:], uint32(len(message)))
	data.Write(messageLengthbytes[:])
	data.Write(message)

	return writeQueryStreamData(w, streamMutex, data.Bytes())
}

func handleQueryStreamFrame(
	request *Request,
	w http.ResponseWriter,
	streamMutex *sync.Mutex,
	framesBuffer *bytes.Buffer,
	databaseKey *database.DatabaseKey,
	accessKey *auth.AccessKey,
) error {
	responseBuffer := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(responseBuffer)
	responseBuffer.Reset()

	responseFramesBuffer := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(responseFramesBuffer)
	responseFramesBuffer.Reset()

	queryBuffer := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(queryBuffer)

	for framesBuffer.Len() > 0 {
		queryBuffer.Reset()

		queryLength := int(binary.LittleEndian.Uint32(framesBuffer.Next(4)))

		queryBuffer.Write(framesBuffer.Next(queryLength))

		responseBytes, err := handleQueryStreamRequest(request, databaseKey, accessKey, queryBuffer)

		if err != nil {
			responseFramesBuffer.Write([]byte{byte(QueryStreamError)})
			var errLengthBytes [4]byte
			binary.LittleEndian.PutUint32(errLengthBytes[:], uint32(len(err.Error())))
			responseFramesBuffer.Write(errLengthBytes[:])
			responseFramesBuffer.Write([]byte(err.Error()))
		} else {
			responseFramesBuffer.Write([]byte{byte(QueryStreamFrameEntry)})
			var responseLengthBytes [4]byte
			binary.LittleEndian.PutUint32(responseLengthBytes[:], uint32(len(responseBytes)))
			responseFramesBuffer.Write(responseLengthBytes[:])
			responseFramesBuffer.Write(responseBytes)
		}
	}

	responseBuffer.WriteByte(uint8(QueryStreamFrame))

	// Write the length of the response
	var responseLengthBytes [4]byte
	binary.LittleEndian.PutUint32(responseLengthBytes[:], uint32(responseFramesBuffer.Len()))
	responseBuffer.Write(responseLengthBytes[:])

	// Write the response
	responseBuffer.Write(responseFramesBuffer.Bytes())

	return writeQueryStreamData(w, streamMutex, responseBuffer.Bytes())
}

func writeQueryStreamData(w http.ResponseWriter, mutex *sync.Mutex, data []byte) error {
	mutex.Lock()
	defer mutex.Unlock()

	_, err := w.Write(data)

	if err != nil {
		log.Println(err)
		return err
	}

	flusher, ok := w.(http.Flusher)

	if !ok {
		log.Println("http.ResponseWriter does not implement http.Flusher")
		return fmt.Errorf("http.ResponseWriter does not implement http.Flusher")
	}

	if flusher == nil {
		log.Println("http.ResponseWriter does not implement http.Flusher")
		return fmt.Errorf("http.ResponseWriter does not implement http.Flusher")
	}

	flusher.Flush()

	return nil
}

// TODO: Implement a write function to handle writing responses to the client
// So that we can buffer more than one response before sending it to the client
func writeQueryStream(
	ctx context.Context,
	w http.ResponseWriter,
	writer chan *bytes.Buffer,
) {
	// TODO: detect the different client connections that have inflight requests
	// and do a best effort to buffer the writes to send as many as possible at
	// once instead of sending one at a time.
	for {
		select {
		case <-ctx.Done():
			return
		case buffer := <-writer:
			_, err := w.Write(buffer.Bytes())
			w.Write([]byte("\n"))

			if err != nil {
				log.Println("Error writing buffer to client", err)
				return
			}

			w.(http.Flusher).Flush()

			bufferPool.Put(buffer)

		}
	}
}
