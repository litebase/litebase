package http

import (
	"bytes"
	"fmt"
	"litebase/server/database"
	"litebase/server/query"
	"log"
	"net/http"

	"github.com/gorilla/websocket"
)

// var bufferPool = sync.Pool{
// 	New: func() interface{} {
// 		return bytes.NewBuffer(make([]byte, 0, 16*1024*1024)) // 1 MiB
// 	},
// }

// // Buffer pool for 1 MiB buffers
// var parameterBufferPool = sync.Pool{
// 	New: func() interface{} {
// 		return make([]interface{}, 999) // 1 MiB
// 	},
// }

func QueryWebsocketController(request *Request) Response {
	return Response{
		StatusCode: 200,
		Stream: func(w http.ResponseWriter) {
			databaseKey, err := database.GetDatabaseKey(request.Subdomains()[0])

			if err != nil {
				w.Write(JsonStringError(fmt.Errorf("a valid database is required to make this request")))
				return
			}

			requestToken := request.RequestToken("Authorization")

			if !requestToken.Valid() {
				w.Write(JsonStringError(fmt.Errorf("a valid access key is required to make this request")))
				log.Println("Request token error")
				return
			}

			accessKey := requestToken.AccessKey(databaseKey.DatabaseUuid)

			if accessKey.AccessKeyId == "" {
				w.Write(JsonStringError(fmt.Errorf("a valid access key is required to make this request")))
				return
			}

			var upgrader = websocket.Upgrader{
				ReadBufferSize:  1024,
				WriteBufferSize: 1024,
			}

			conn, err := upgrader.Upgrade(w, request.BaseRequest, nil)

			if err != nil {
				log.Println("Error upgrading to websocket", err)
				return
			}

			defer conn.Close()

			responseBuffer := bufferPool.Get().(*bytes.Buffer)
			scannedTextBuffer := bufferPool.Get().(*bytes.Buffer)

			defer bufferPool.Put(responseBuffer)
			defer bufferPool.Put(scannedTextBuffer)

			var command *query.QueryInput

			response := &query.QueryResponse{}

			for {
				err := conn.ReadJSON(&command)

				if err != nil {
					// Handle error
					log.Println("Error unmarshalling command", err)
					conn.WriteJSON(JsonStringError(err))
					break
				}
				// log.Println("Message", string(message))

				// n, _ := scannedTextBuffer.Write(message)

				// err = json.Unmarshal(scannedTextBuffer.Next(n), &command)

				response.Reset()

				err = processInput(databaseKey, accessKey, command, response)

				if err != nil {
					log.Println("Error processing command", err)
					conn.WriteJSON(JsonStringError(err))
					return
				}

				// n, err := responseBuffer.Write(response.ToMap())

				// if err != nil {
				// 	log.Println("Error writing response", err)
				// 	conn.WriteJSON(JsonStringError(err))
				// 	return
				// }

				// x := responseBuffer.Next(n)
				err = conn.WriteJSON(query.QueryJsonResponse{
					Status: "success",
					Data:   response,
				})
				// log.Println("Response", string(x))
				// err = conn.WriteMessage(messageType, x)

				if err != nil {
					log.Println("Error writing response", err)
					conn.WriteJSON(JsonStringError(err))

					return
				}
			}

			// 	w.Header().Set("Transfer-Encoding", "chunked")
			// 	w.Header().Set("Connection", "close")
			// 	w.Header().Set("Content-Type", "text/plain")

			// 	defer request.BaseRequest.Body.Close()

			// 	scanner := bufio.NewScanner(request.BaseRequest.Body)

			// 	for scanner.Scan() {
			// 		scannedTextBuffer.Reset()
			// 		responseBuffer.Reset()

			// 		n, _ := scannedTextBuffer.Write(scanner.Bytes())

			// 		err := json.Unmarshal(scannedTextBuffer.Next(n), &command)

			// 		if err != nil {
			// 			w.Write(JsonStringError(err))
			// 			w.(http.Flusher).Flush()
			// 			return
			// 		}

			// 		n, err = processCommand(databaseHash, databaseKey, accessKey, command, responseBuffer)

			// 		if err != nil {
			// 			w.Write(JsonStringError(err))
			// 			w.(http.Flusher).Flush()
			// 			return
			// 		}

			// 		_, err = w.Write(responseBuffer.Next(n))

			// 		if err != nil {
			// 			log.Println("Error writing response", err)
			// 			w.Write(JsonStringError(err))

			// 			return
			// 		}

			// 		w.Write([]byte("\n"))

			// 		w.(http.Flusher).Flush()
			// 	}

		},
	}
}
