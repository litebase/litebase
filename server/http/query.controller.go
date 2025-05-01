package http

import (
	"github.com/litebase/litebase/server/database"
)

func QueryController(request *Request) Response {
	databaseKey := request.DatabaseKey()

	if databaseKey == nil {
		return ErrValidDatabaseKeyRequiredResponse
	}

	requestToken := request.RequestToken("Authorization")

	if !requestToken.Valid() {
		return ErrInvalidAccessKeyResponse
	}

	accessKey := requestToken.AccessKey(databaseKey.DatabaseId)

	if accessKey.AccessKeyId == "" {
		return ErrInvalidAccessKeyResponse
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

	defer request.databaseManager.ConnectionManager().Release(
		databaseKey.DatabaseId,
		databaseKey.BranchId,
		db,
	)

	queryInput := &database.QueryInput{}

	err = queryInput.DecodeFromMap(request.All())

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"message": err.Error(),
		}, 500, nil)
	}

	requestQuery := database.GetQuery(
		request.cluster,
		request.databaseManager,
		request.logManager,
		databaseKey,
		accessKey,
		queryInput,
	)

	defer database.PutQuery(requestQuery)

	response := &database.QueryResponse{}

	if requestQuery.Input.TransactionId != nil &&
		!requestQuery.IsTransactionEnd() &&
		!requestQuery.IsTransactionRollback() {
		transaction, err := request.databaseManager.Resources(
			databaseKey.DatabaseId,
			databaseKey.BranchId,
		).TransactionManager().Get(string(requestQuery.Input.TransactionId))

		if err != nil {
			return JsonResponse(map[string]interface{}{
				"message": err.Error(),
			}, 500, nil,
			)
		}

		if accessKey.AccessKeyId != transaction.AccessKey.AccessKeyId {
			return ErrInvalidAccessKeyResponse
		}

		err = transaction.ResolveQuery(requestQuery, response)

		if err != nil {
			return JsonResponse(map[string]interface{}{
				"message": err.Error(),
			}, 500, nil,
			)
		}
	} else {
		_, err = requestQuery.Resolve(response)

		if err != nil {
			return JsonResponse(map[string]interface{}{
				"message": err.Error(),
			}, 500, nil)
		}
	}

	return Response{
		StatusCode: 200,
		Body:       response.ToMap(),
	}
}
