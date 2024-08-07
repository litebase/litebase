package http

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"litebase/server/auth"
	"litebase/server/database"
	"litebase/server/file"
	"litebase/server/node"
	"litebase/server/query"
	"log"
	"net/http"
	"sync"
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 1*1024*1024)) // 1 MiB
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

			responseBuffer := bufferPool.Get().(*bytes.Buffer)
			scannedTextBuffer := bufferPool.Get().(*bytes.Buffer)

			defer bufferPool.Put(responseBuffer)
			defer bufferPool.Put(scannedTextBuffer)

			var command *query.QueryInput
			var databaseHash = file.DatabaseHash(databaseKey.DatabaseUuid, databaseKey.BranchUuid)
			scanner := bufio.NewScanner(request.BaseRequest.Body)

			for scanner.Scan() {
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

				err := json.Unmarshal(scannedTextBuffer.Next(n), &command)

				if err != nil {
					w.Write(JsonNewLineError(err))
					w.(http.Flusher).Flush()
					return
				}

				response, err := processCommand(databaseHash, databaseKey, accessKey, command)

				if err != nil {
					w.Write(JsonNewLineError(err))
					w.(http.Flusher).Flush()
					return
				}

				data, err := response.ToJSON()

				if err != nil {
					w.Write(JsonNewLineError(err))
					w.(http.Flusher).Flush()
					return
				}

				n, err = responseBuffer.Write(data)

				if err != nil {
					w.Write(JsonNewLineError(err))
					w.(http.Flusher).Flush()

					return
				}

				_, err = w.Write(responseBuffer.Next(n))

				if err != nil {
					log.Println("Error writing response", err)
					w.Write(JsonNewLineError(err))

					return
				}

				w.Write([]byte("\n"))

				w.(http.Flusher).Flush()
			}
		},
	}
}

func processCommand(
	databaseHash string,
	databaseKey database.DatabaseKey,
	accessKey auth.AccessKey,
	input *query.QueryInput,
) (node.NodeQueryResponse, error) {
	requestQuery, err := query.NewQuery(
		databaseKey.DatabaseUuid,
		databaseKey.BranchUuid,
		accessKey,
		input.Statement,
		input.Parameters,
		input.Id,
	)

	if err != nil {
		return nil, err
	}

	response, err := requestQuery.Resolve(databaseHash)

	if err != nil {
		log.Println("Error resolving query", err)
		return nil, err
	}

	return response, nil
}
