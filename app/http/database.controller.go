package http

import (
	"litebasedb/runtime/app/database"
	"os"
)

type DatabaseController struct {
}

func (controller *DatabaseController) Destroy(request *Request) *Response {
	directory := database.GetFileDir(request.Param("database"), request.Param("branch"))
	os.RemoveAll(directory)
	database.EnsureDatabaseExists(request.Param("database"), request.Param("branch"))

	return JsonResponse(map[string]interface{}{
		"status":  "success",
		"message": "Database deleted successfully.",
	}, 200, nil)
}
