package http

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"sync"

	"github.com/litebase/litebase/internal/utils"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/database"
)

var bufferPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 1024))
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
	databaseKey := request.DatabaseKey()

	if databaseKey == nil {
		return ErrValidDatabaseKeyRequiredResponse
	}

	requestToken := request.RequestToken("Authorization")

	if !requestToken.Valid() {
		return BadRequestResponse(fmt.Errorf("a valid access key is required to make this request"))
	}

	accessKey := requestToken.AccessKey()

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
	databaseKey *auth.DatabaseKey,
	accessKey *auth.AccessKey,
	input *database.QueryInput,
	response cluster.NodeQueryResponse,
) error {
	var err error
	var transaction *database.Transaction

	requestQuery := database.GetQuery(
		request.cluster,
		request.databaseManager,
		request.logManager,
		databaseKey,
		accessKey,
		input,
	)

	defer database.PutQuery(requestQuery)

	if requestQuery.Input.TransactionId != nil &&
		!requestQuery.IsTransactionEnd() &&
		!requestQuery.IsTransactionRollback() {
		transaction, err = request.databaseManager.Resources(
			databaseKey.DatabaseId,
			databaseKey.BranchId,
		).TransactionManager().Get(string(requestQuery.Input.TransactionId))

		if err != nil {
			return err
		}

		if accessKey.AccessKeyId != transaction.AccessKey.AccessKeyId {
			return fmt.Errorf("invalid access key")
		}

		err = transaction.ResolveQuery(requestQuery, response.(*database.QueryResponse))
	} else {
		_, err = requestQuery.Resolve(response)
	}

	return err
}

func readQueryStream(
	cancel context.CancelFunc,
	request *Request,
	w http.ResponseWriter,
	databaseKey *auth.DatabaseKey,
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

		messageType := int(messageHeaderBytes[0])

		// Read the message length
		messageLength := int(binary.LittleEndian.Uint32(messageHeaderBytes[1:]))

		// Read the message in chunks
		bytesRead := 0

		for bytesRead < messageLength {
			chunkSize := min(messageLength-bytesRead, 1024)

			n, err := io.CopyN(scanBuffer, request.BaseRequest.Body, int64(chunkSize))

			if err != nil {
				log.Println(err)
				break
			}

			bytesRead += int(n)
		}

		switch QueryStreamMessageType(messageType) {
		case QueryStreamOpenConnection:
			err := handleQueryStreamConnection(w, streamMutex)

			if err != nil {
				log.Println(err)
				return
			}

			// continue
		case QueryStreamCloseConnection:
			cancel()
			return
		case QueryStreamFrame:
			queryStreamFrameBuffer := bufferPool.Get().(*bytes.Buffer)
			queryStreamFrameBuffer.Reset()

			err := handleQueryStreamFrame(request, w, streamMutex, scanBuffer, databaseKey, accessKey)

			if err != nil {
				slog.Error("Error handling query stream frame", "error", err)
			}

			bufferPool.Put(queryStreamFrameBuffer)
		default:
			log.Println("Unknown message type", messageType)
			// return
		}
	}
}

func handleQueryStreamRequest(
	request *Request,
	databaseKey *auth.DatabaseKey,
	accessKey *auth.AccessKey,
	queryData *bytes.Buffer,
	queryParameters *bytes.Buffer,
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

	err := queryInput.Decode(queryData, queryParameters)

	if err != nil {
		return nil, err
	}

	err = processInput(request, databaseKey, accessKey, queryInput, response)
	responseBytes, _ := response.Encode(responseBuffer, rowsBuffer, columnsBuffer)

	return responseBytes, err
}

func handleQueryStreamConnection(w http.ResponseWriter, streamMutex *sync.Mutex) error {
	message := []byte("connected")
	data := bytes.NewBuffer(make([]byte, 0))
	data.WriteByte(uint8(QueryStreamOpenConnection))

	var messageLengthBytes [4]byte

	uint32MessageLength, err := utils.SafeIntToUint32(len(message))

	if err != nil {
		return err
	}

	binary.LittleEndian.PutUint32(messageLengthBytes[:], uint32MessageLength)
	data.Write(messageLengthBytes[:])
	data.Write(message)

	return writeQueryStreamData(w, streamMutex, data.Bytes())
}

func handleQueryStreamFrame(
	request *Request,
	w http.ResponseWriter,
	streamMutex *sync.Mutex,
	framesBuffer *bytes.Buffer,
	databaseKey *auth.DatabaseKey,
	accessKey *auth.AccessKey,
) error {
	// The responseBuffer contains multiple frames
	responseBuffer := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(responseBuffer)
	responseBuffer.Reset()

	// The responseFramesBuffer contains multiple frame entries
	responseFramesBuffer := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(responseFramesBuffer)
	responseFramesBuffer.Reset()

	queryBuffer := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(queryBuffer)

	queryParamsBuffer := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(queryParamsBuffer)

	for framesBuffer.Len() > 0 {
		queryBuffer.Reset()
		queryParamsBuffer.Reset()

		queryLength := int(binary.LittleEndian.Uint32(framesBuffer.Next(4)))
		queryBuffer.Write(framesBuffer.Next(queryLength))

		responseBytes, err := handleQueryStreamRequest(request, databaseKey, accessKey, queryBuffer, queryParamsBuffer)

		if err != nil {
			// Write the type of message
			responseFramesBuffer.WriteByte(uint8(QueryStreamError))
		} else {
			// Write the type of message
			responseFramesBuffer.WriteByte(uint8(QueryStreamFrameEntry))
		}

		// Write the length of the response
		var responseLengthBytes [4]byte

		uint32ResponseBytesLength, err := utils.SafeIntToUint32(len(responseBytes))

		if err != nil {
			return err
		}

		binary.LittleEndian.PutUint32(responseLengthBytes[:], uint32ResponseBytesLength)

		// Write the length of the response
		responseFramesBuffer.Write(responseLengthBytes[:])
		// Write the response
		responseFramesBuffer.Write(responseBytes)
	}

	// Write the type of message
	responseBuffer.WriteByte(uint8(QueryStreamFrame))
	// Write the length of the response
	var responseLengthBytes [4]byte

	uint32ResponseFramesBufferLength, err := utils.SafeIntToUint32(responseFramesBuffer.Len())

	if err != nil {
		return err
	}

	binary.LittleEndian.PutUint32(responseLengthBytes[:], uint32ResponseFramesBufferLength)
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
