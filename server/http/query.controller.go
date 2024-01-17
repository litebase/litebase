package http

import (
	"fmt"
	"litebasedb/server/counter"
	"litebasedb/server/database"
	"litebasedb/server/query"
)

func QueryController(request *Request) *Response {
	key := request.Subdomains()[0]
	// log.Println("databaseKey", databaseKey, len(request.Subdomains()))
	if key == "" || len(request.Subdomains()) != 2 {
		return BadRequestResponse(fmt.Errorf("this request is not valid"))
	}

	databaseKey, err := database.GetDatabaseKey(key)

	if databaseKey == nil || err != nil {
		return BadRequestResponse(fmt.Errorf("a valid database is required to make this request"))
	}

	requestToken := request.RequestToken("Authorization")

	if requestToken == nil {
		return BadRequestResponse(fmt.Errorf("a valid access key is required to make this request"))
	}

	accessKey := requestToken.AccessKey(databaseKey.DatabaseUuid)

	if accessKey == nil {
		return BadRequestResponse(fmt.Errorf("a valid access key is required to make this request"))
	}

	db, err := database.ConnectionManager().Get(
		databaseKey.DatabaseUuid,
		databaseKey.BranchUuid,
	)

	defer database.ConnectionManager().Release(
		databaseKey.DatabaseUuid,
		databaseKey.BranchUuid,
		db,
	)

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"message": err.Error(),
		}, 500, nil)
	}

	requestQuery, err := query.NewQuery(
		db.WithAccessKey(accessKey),
		accessKey.AccessKeyId,
		request.All(),
		"",
	)

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"message": err.Error(),
		}, 500, nil)
	}

	response := query.NewResolver().Handle(db, requestQuery)

	defer counter.Increment(databaseKey.DatabaseUuid, databaseKey.BranchUuid)

	return &Response{
		StatusCode: 200,
		Body:       response,
	}
}
