package http

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
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
			w.Header().Set("Content-Type", "text/plain")
			w.Header().Set("Transfer-Encoding", "chunked")

			defer request.BaseRequest.Body.Close()

			ctx, cancel := context.WithCancel(context.Background())
			scanner := bufio.NewScanner(request.BaseRequest.Body)
			writer := make(chan *bytes.Buffer)

			go readQueryStream(cancel, request, scanner, databaseKey, accessKey, writer)
			go writeQueryStream(ctx, w, writer)

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
	scanner *bufio.Scanner,
	databaseKey *database.DatabaseKey,
	accessKey *auth.AccessKey,
	writer chan *bytes.Buffer,
) {
	errorBuffer := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(errorBuffer)

	errorBuffer.Reset()

	for {
		if err := scanner.Err(); err != nil {
			break
		}

		if scanner.Scan() {
			if err := scanner.Err(); err != nil {
				errorBuffer.WriteString(err.Error())
				writer <- errorBuffer
				break
			}

			errorBuffer.Reset()

			if err := scanner.Err(); err != nil {
				errorBuffer.WriteString(err.Error())
				writer <- errorBuffer
				break
			}

			scanBuffer := bufferPool.Get().(*bytes.Buffer)
			scanBuffer.Reset()
			scanBuffer.Write(scanner.Bytes())

			go scan(request, databaseKey, accessKey, scanBuffer, writer)
		} else {
			break
		}
	}

	cancel()
}

func scan(
	request *Request,
	databaseKey *database.DatabaseKey,
	accessKey *auth.AccessKey,
	scanBuffer *bytes.Buffer,
	writer chan *bytes.Buffer,
) {
	writeBuffer := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(scanBuffer)
	writeBuffer.Reset()

	n := scanBuffer.Len()

	// TODO: We need to handle a connection event. NodeJS doesn't start
	// the request without any data being sent first.
	if n == 0 {
		writeBuffer.Write([]byte(`{"connected": true}` + "\n"))
		writer <- writeBuffer
		return
	}

	var err error

	response := database.ResponsePool().Get()
	defer database.ResponsePool().Put(response)

	jsonResponse := &database.QueryJsonResponse{}

	input := inputPool.Get().(*database.QueryInput)
	defer inputPool.Put(input)

	decoder := database.JsonDecoderPool().Get()
	defer database.JsonDecoderPool().Put(decoder)

	decoder.Buffer.Write(scanBuffer.Bytes())

	err = decoder.JsonDecoder.Decode(input)

	if err != nil {
		writeBuffer.Write(JsonNewLineErrorWithData(err, map[string]interface{}{
			"id": input.Id,
		}))
		writer <- writeBuffer
		return
	}

	response.Reset()

	err = processInput(request, databaseKey, accessKey, input, response)

	if err != nil {
		writeBuffer.Write(JsonNewLineErrorWithData(err, map[string]interface{}{
			"id": input.Id,
		}))
		writer <- writeBuffer
		return
	}

	jsonResponse.Status = "success"
	jsonResponse.Data = response

	encoder := database.JsonEncoderPool().Get()
	defer database.JsonEncoderPool().Put(encoder)

	encoder.Buffer.Reset()

	err = encoder.JsonEncoder.Encode(jsonResponse)

	if err != nil {
		writeBuffer.Write(JsonNewLineErrorWithData(err, map[string]interface{}{
			"id": input.Id,
		}))
		writer <- writeBuffer
		return
	}

	writeBuffer.Write(encoder.Buffer.Bytes())

	writer <- writeBuffer
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
