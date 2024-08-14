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

	databaseUuid := request.DatabaseKey().DatabaseUuid
	branchUuid := request.DatabaseKey().BranchUuid
	err := accessKey.CanAccess(databaseUuid, branchUuid)

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
