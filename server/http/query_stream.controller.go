package http

import (
	"bufio"
	"encoding/json"
	"fmt"
	"litebasedb/server/auth"
	"litebasedb/server/database"
	"litebasedb/server/query"
	"log"
	"net/http"
)

func QueryStreamController(request *Request) Response {
	return Response{
		StatusCode: 200,
		Stream: func(w http.ResponseWriter) {
			w.Header().Set("Transfer-Encoding", "chunked")
			w.Header().Set("Connection", "close")

			w.(http.Flusher).Flush()

			databaseKey, err := database.GetDatabaseKey(request.Subdomains()[0])

			if err != nil {
				w.Write(JsonNewLineError(fmt.Errorf("a valid database is required to make this request")))
				log.Println("Database key error", err)
				return
			}

			requestToken := request.RequestToken("Authorization")

			if !requestToken.Valid() {
				w.Write(JsonNewLineError(fmt.Errorf("a valid access key is required to make this request")))
				log.Println("Request token error")
				return
			}

			accessKey := requestToken.AccessKey(databaseKey.DatabaseUuid)

			if accessKey.AccessKeyId == "" {
				w.Write(JsonNewLineError(fmt.Errorf("a valid access key is required to make this request")))
				log.Println("Access key error")
				return
			}

			db, err := database.ConnectionManager().Get(
				databaseKey.DatabaseUuid,
				databaseKey.BranchUuid,
			)

			if err != nil {
				w.Write(JsonNewLineError(err))
				w.(http.Flusher).Flush()
				log.Println("Database connection error", err)

				if db != nil {
					database.ConnectionManager().Remove(
						databaseKey.DatabaseUuid,
						databaseKey.BranchUuid,
						db,
					)
				}

				return
			}

			scanner := bufio.NewScanner(request.BaseRequest.Body)

			for scanner.Scan() {
				command := scanner.Text()

				response, err := processCommand(db, accessKey, command)

				if err != nil {
					w.Write(JsonNewLineError(err))
					return
				}

				jsonResponse, err := json.Marshal(response)

				if err != nil {
					w.Write(JsonNewLineError(err))
					log.Println("JSON error", err)
					return
				}

				w.Write(jsonResponse)
				w.(http.Flusher).Flush()
			}

			request.BaseRequest.Body.Close()
			<-request.BaseRequest.Context().Done()

			database.ConnectionManager().Release(
				databaseKey.DatabaseUuid,
				databaseKey.BranchUuid,
				db,
			)
		},
	}
}

func processCommand(db *database.ClientConnection, accessKey auth.AccessKey, command string) (map[string]interface{}, error) {
	var queryCommand map[string]interface{}

	err := json.Unmarshal([]byte(command), &queryCommand)

	if err != nil {
		return nil, err
	}

	requestQuery, err := query.NewQuery(
		db.WithAccessKey(accessKey),
		accessKey.AccessKeyId,
		queryCommand,
		"",
	)

	if err != nil {
		return nil, err
	}

	response, err := requestQuery.Resolve()

	if err != nil {
		return nil, err
	}

	// if response["status"].(string) == "error" {
	// 	return nil, errors.New(response["message"].(string))
	// }

	// defer counter.Increment(databaseKey.DatabaseUuid, databaseKey.BranchUuid)

	return response, nil
}
