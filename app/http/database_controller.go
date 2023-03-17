package http

import (
	"litebasedb/app/database"
	"litebasedb/app/file"
	"os"
)

func DatabaseDestroyController(request *Request) *Response {
	directory := file.GetFileDir(request.Param("database"), request.Param("branch"))
	os.RemoveAll(directory)
	database.EnsureDatabaseExists(request.Param("database"), request.Param("branch"))

	return JsonResponse(map[string]interface{}{
		"status":  "success",
		"message": "Database deleted successfully.",
	}, 200, nil)
}
