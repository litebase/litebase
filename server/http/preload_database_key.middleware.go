package http

func PreloadDatabaseKey(request *Request) (*Request, Response) {
	request.loadDatabaseKey()

	return request, Response{}
}
