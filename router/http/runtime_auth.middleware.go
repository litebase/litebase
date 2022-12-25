package http

func RuntimeAuth(request *Request) (*Request, *Response) {
	if !ensureReuestHasAnAuthorizationHeader(request) || !ensureRuntimeRequestIsProperlySigned(request) {
		return nil, &Response{
			StatusCode: 401,
			Body: map[string]interface{}{
				"status":  "error",
				"message": "Unauthorized",
			},
		}
	}

	if !ensureAuthRequestIsNotExpired(request) {
		return nil, &Response{
			StatusCode: 401,
			Body: map[string]interface{}{
				"status":  "error",
				"message": "Unauthorized",
			},
		}
	}

	return request, nil
}

func ensureRuntimeRequestIsProperlySigned(request *Request) bool {
	databaseUuid := request.Param("databaseUuid")
	branchUuid := request.Param("branchUuid")
	return RuntimeRequestSignatureValidator(request, databaseUuid, branchUuid)
}
