package http

import (
	"encoding/json"
	"fmt"
	"litebase/server/database"
)

func QueryController(request *Request) Response {
	databaseKey, err := database.GetDatabaseKey(
		request.cluster.Config,
		request.cluster.ObjectFS(),
		request.Subdomains()[0],
	)

	if err != nil {
		return BadRequestResponse(fmt.Errorf("a valid database is required to make this request"))
	}

	requestToken := request.RequestToken("Authorization")

	if !requestToken.Valid() {
		return BadRequestResponse(fmt.Errorf("a valid access key is required to make this request"))
	}

	accessKey := requestToken.AccessKey(databaseKey.DatabaseId)

	if accessKey.AccessKeyId == "" {
		return BadRequestResponse(fmt.Errorf("a valid access key is required to make this request"))
	}

	db, err := request.databaseManager.ConnectionManager().Get(
		databaseKey.DatabaseId,
		databaseKey.BranchId,
	)

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"message": err.Error(),
		}, 500, nil)
	}
	queryInput := &database.QueryInput{}

	err = json.NewDecoder(request.BaseRequest.Body).Decode(queryInput)

	if err != nil {
		request.databaseManager.ConnectionManager().Remove(
			databaseKey.DatabaseId,
			databaseKey.BranchId,
			db,
		)

		return JsonResponse(map[string]interface{}{
			"message": err.Error(),
		}, 500, nil)
	}

	requestQuery, err := database.NewQuery(
		request.cluster,
		request.databaseManager,
		databaseKey,
		accessKey,
		queryInput,
	)

	if err != nil {
		request.databaseManager.ConnectionManager().Remove(
			databaseKey.DatabaseId,
			databaseKey.BranchId,
			db,
		)

		return JsonResponse(map[string]interface{}{
			"message": err.Error(),
		}, 500, nil)
	}

	response := &database.QueryResponse{}

	_, err = requestQuery.Resolve(response)

	if err != nil {
		request.databaseManager.ConnectionManager().Remove(
			databaseKey.DatabaseId,
			databaseKey.BranchId,
			db,
		)

		return JsonResponse(map[string]interface{}{
			"message": err.Error(),
		}, 500, nil)
	}

	// defer counter.Increment(databaseKey.DatabaseId, databaseKey.BranchId)

	defer request.databaseManager.ConnectionManager().Release(
		databaseKey.DatabaseId,
		databaseKey.BranchId,
		db,
	)

	return Response{
		StatusCode: 200,
		Body:       response.ToMap(),
	}
}
