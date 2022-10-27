package http

import (
	"litebasedb/runtime/config"
	"litebasedb/runtime/database"
	"litebasedb/runtime/query"
)

type QueryController struct {
}

func (controller *QueryController) Store(request *Request) *Response {
	code := 200

	db, err := database.Get(
		config.Get("database_uuid"),
		config.Get("branch_uuid"),
		request.RequestToken("Authorization").AccessKey(),
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

	return JsonResponse(response, code, nil)
}
