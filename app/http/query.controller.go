package http

import (
	"litebasedb/app/auth"
	"litebasedb/app/counter"
	"litebasedb/app/database"
	"litebasedb/app/query"
	"litebasedb/internal/config"
	"log"
)

func QueryController(request *Request) *Response {
	databaseKey := request.Subdomains()[0]
	log.Println("test")
	if databaseKey == "" || len(request.Subdomains()) != 2 {
		return &Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"status":  "error",
				"message": "Bad request",
			},
		}
	}

	databaseUuid := auth.SecretsManager().GetDatabaseUuid(databaseKey)

	if databaseUuid == "" {
		return &Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"status":  "error",
				"message": "Bad request",
			},
		}
	}

	accessKey := request.RequestToken("Authorization").AccessKey(databaseUuid)

	if accessKey == nil {
		return &Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"status":  "error",
				"message": "Bad request",
			},
		}
	}

	branchUuid := accessKey.GetBranchUuid()

	if branchUuid == "" {
		return &Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"status":  "error",
				"message": "Bad request",
			},
		}
	}

	db, err := database.Get(
		config.Get("database_uuid"),
		config.Get("branch_uuid"),
		request.RequestToken("Authorization").AccessKey(request.headers.Get("X-Lbdb-Signature")),
		false,
	)

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"message": err.Error(),
		}, 500, nil)
	}

	requestQuery, err := query.NewQuery(
		db,
		request.RequestToken("Authorization").AccessKeyId,
		request.All(),
		"",
	)

	if err != nil {
		return JsonResponse(map[string]interface{}{
			// Todo: Add proper error message
			"message": err.Error(),
		}, 500, nil)
	}

	resolver := query.NewResolver()
	response := resolver.Handle(db, requestQuery, false)

	// TODO: Only incrment on successfull response
	defer counter.Increment(databaseUuid, branchUuid)

	return &Response{
		StatusCode: 200,
		Body:       response,
	}
}
