package http

import "litebasedb/runtime/auth"

type SignatureNextController struct {
}

func (controller *SignatureNextController) Store(request *Request) *Response {
	if request.Get("signature") == nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": "A signature is required",
		}, 400, nil)
	}

	publickey := auth.NextSignature(request.Get("signature").(string))

	return JsonResponse(map[string]interface{}{
		"status":  "success",
		"message": "Database settings stored successfully",
		"data": map[string]string{
			"publick_key": publickey,
		},
	}, 200, nil)
}
