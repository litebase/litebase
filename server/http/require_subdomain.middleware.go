package http

import "fmt"

func RequireSubdomain(request *Request) (*Request, Response) {
	if len(request.Subdomains()) < 2 {
		return request, BadRequestResponse(fmt.Errorf("this request is not valid"))
	}

	return request, Response{}
}
