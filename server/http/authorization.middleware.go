package http

func Authorization(request *Request) (*Request, Response) {
	accessKey := request.RequestToken("Authorization").AccessKey(request.Subdomains()[0])

	if accessKey == nil {
		return request, Response{
			StatusCode: 401,
			Body: map[string]interface{}{
				"status":  "error",
				"message": "Unauthorized",
			},
		}
	}

	databaseKey := request.DatabaseKey()

	if databaseKey == nil {
		return request, Response{
			StatusCode: 404,
			Body: map[string]interface{}{
				"status":  "error",
				"message": "Database not found",
			},
		}
	}

	databaseId := request.DatabaseKey().DatabaseId
	branchId := request.DatabaseKey().BranchId
	err := accessKey.CanAccessDatabase(databaseId, branchId)

	if err != nil {
		return request, Response{
			StatusCode: 401,
			Body: map[string]interface{}{
				"status":  "error",
				"message": err.Error(),
			},
		}
	}

	return request, Response{}
}
