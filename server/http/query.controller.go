package http

import (
	"fmt"
	"litebasedb/server/database"
	"litebasedb/server/query"
)

func QueryController(request *Request) *Response {
	// start := time.Now()
	// defer func() {
	// 	log.Println("QueryController", time.Since(start))
	// }()

	databaseKey, err := database.GetDatabaseKey(request.Subdomains()[0])

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

	response, err := query.NewResolver().Handle(db, requestQuery)

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"message": err.Error(),
		}, 500, nil)
	}
	// defer counter.Increment(databaseKey.DatabaseUuid, databaseKey.BranchUuid)

	return &Response{
		StatusCode: 200,
		Body:       response,
	}
}
