package http

import (
	"litebasedb/internal/config"
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
		request.RequestToken("Authorization").AccessKey(request.headers.Get("X-Lbdb-Signature")),
		false,
	)

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"message": err.Error(),
		}, 500, nil)
	}

	requestQuery, err := query.NewQuery(
		request.headers.Get("X-Lbdb-Signature"),
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

	response := resolver.Handle(request.headers.Get("X-Lbdb-Signature"), db, requestQuery, false)

	return JsonResponse(response, code, nil)
}
