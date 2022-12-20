package http

import "log"

func RuntimeAuth(request *Request) (*Request, *Response) {
	log.Println("-----Runtime auth-----")
	return request, nil
}
