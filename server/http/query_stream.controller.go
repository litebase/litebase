package http

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"litebase/server/auth"
	"litebase/server/database"
	"litebase/server/query"
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
var inputPool = sync.Pool{
	New: func() interface{} {
		return &query.QueryInput{}
	},
}

func QueryStreamController(request *Request) Response {
	databaseKey, err := database.GetDatabaseKey(request.Subdomains()[0])

	if err != nil {
		return BadRequestResponse(fmt.Errorf("a valid database is required to make this request"))
	}

	requestToken := request.RequestToken("Authorization")

	if !requestToken.Valid() {
		return BadRequestResponse(fmt.Errorf("a valid access key is required to make this request"))
	}

	accessKey := requestToken.AccessKey(databaseKey.DatabaseUuid)

	if accessKey.AccessKeyId == "" {
		return BadRequestResponse(fmt.Errorf("a valid access key is required to make this request"))
	}

	return Response{
		StatusCode: 200,
		Stream: func(w http.ResponseWriter) {
			w.Header().Set("Transfer-Encoding", "chunked")
			w.Header().Set("Connection", "close")
			w.Header().Set("Content-Type", "text/plain")

			defer request.BaseRequest.Body.Close()

			scannedTextBuffer := bufferPool.Get().(*bytes.Buffer)
			requestBuffer := bufferPool.Get().(*bytes.Buffer)
			responseBuffer := bufferPool.Get().(*bytes.Buffer)

			defer bufferPool.Put(scannedTextBuffer)
			defer bufferPool.Put(requestBuffer)
			defer bufferPool.Put(responseBuffer)

			var input *query.QueryInput
			var err error

			var decoder = json.NewDecoder(requestBuffer)
			var encoder = json.NewEncoder(responseBuffer)

			scanner := bufio.NewScanner(request.BaseRequest.Body)
			response := &query.QueryResponse{}
			jsonResponse := &query.QueryJsonResponse{}

			for scanner.Scan() {
				requestBuffer.Reset()
				responseBuffer.Reset()
				scannedTextBuffer.Reset()

				n, _ := scannedTextBuffer.Write(scanner.Bytes())

				// TODO: We need to handle a connection event. NodeJS doesn't start
				// the request without any data being sent first.
				if n == 0 {
					w.Write([]byte(`{"connected": true}` + "\n"))
					w.(http.Flusher).Flush()
					continue
				}

				_, err = requestBuffer.Write(scannedTextBuffer.Next(n))

				if err != nil {
					w.Write(JsonNewLineError(err))
					w.(http.Flusher).Flush()
					return
				}

				input = inputPool.Get().(*query.QueryInput)

				err = decoder.Decode(&input)

				if err != nil {
					w.Write(JsonNewLineError(err))
					w.(http.Flusher).Flush()
					return
				}

				response.Reset()

				err = processInput(databaseKey, accessKey, input, response)

				inputPool.Put(input)

				if err != nil {
					w.Write(JsonNewLineError(err))
					w.(http.Flusher).Flush()
					return
				}

				jsonResponse.Status = "success"
				jsonResponse.Data = response

				err = encoder.Encode(jsonResponse)

				if err != nil {
					w.Write(JsonNewLineError(err))
					w.(http.Flusher).Flush()
					return
				}

				// n, err = responseBuffer.Write(data)

				// if err != nil {
				// 	w.Write(JsonNewLineError(err))
				// 	w.(http.Flusher).Flush()

				// 	return
				// }

				_, err = w.Write(responseBuffer.Bytes())

				if err != nil {
					log.Println("Error writing response", err)
					w.Write(JsonNewLineError(err))

					return
				}

				// w.Write([]byte("\n"))

				w.(http.Flusher).Flush()

			}
		},
	}
}

func processInput(
	databaseKey *database.DatabaseKey,
	accessKey *auth.AccessKey,
	input *query.QueryInput,
	response *query.QueryResponse,
) error {
	requestQuery := query.Get(
		databaseKey,
		accessKey,
		input,
	)

	defer query.Put(requestQuery)

	err := requestQuery.ResolveQuery(response)

	if err != nil {
		log.Println("Error resolving query", err)
		return err
	}

	return nil
}
