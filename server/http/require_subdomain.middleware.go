package http

import "fmt"

func RequireSubdomain(request *Request) (*Request, *Response) {
	if len(request.Subdomains()) < 2 {
		return nil, BadRequestResponse(fmt.Errorf("this request is not valid"))
	}

	return request, nil
}
