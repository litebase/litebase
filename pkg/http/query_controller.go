package http

import (
	"fmt"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/database"
	"golang.org/x/exp/slog"
)

func QueryController(request *Request) Response {
	databaseKey, errResponse := request.DatabaseKey()

	if !errResponse.IsEmpty() {
		return errResponse
	}

	requestToken := request.RequestToken("Authorization")

	if !requestToken.Valid() {
		return ErrInvalidAccessKeyResponse
	}

	accessKey := requestToken.AccessKey()

	if accessKey == nil {
		return ErrInvalidAccessKeyResponse
	}

	// Authorize the request
	err := request.Authorize(
		[]string{fmt.Sprintf("database:%s:branch:%s", databaseKey.DatabaseName, databaseKey.DatabaseBranchName)},
		[]auth.Privilege{auth.DatabasePrivilegeQuery},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	db, err := request.databaseManager.ConnectionManager().Get(
		databaseKey.DatabaseID,
		databaseKey.DatabaseBranchID,
	)

	if err != nil {
		return ServerErrorResponse(err)
	}

	defer request.databaseManager.ConnectionManager().Release(db)

	input := &database.QueryInput{}

	err = input.DecodeFromMap(request.All())

	if err != nil {
		slog.Error("failed to parse input", "error", err.Error())
		return BadRequestResponse(ErrInvalidInput)
	}

	// Validate the input
	validationErrors := request.Validate(input, map[string]string{
		"id.required":                 "The query ID field is required.",
		"parameters.required":         "The parameters field is required.",
		"parameters.*.type.required":  "The parameter type field is required.",
		"parameters.*.type.oneof":     "The parameter type field must be one of the allowed values.",
		"parameters.*.value.required": "The parameter value field is required.",
		"statement.required":          "The SQL statement field is required.",
	})

	if validationErrors != nil {
		return ValidationErrorResponse(validationErrors)
	}

	requestQuery := database.GetQuery(
		request.cluster,
		request.databaseManager,
		request.logManager,
		databaseKey,
		accessKey,
		input,
	)

	defer database.PutQuery(requestQuery)

	response := &database.QueryResponse{}

	if requestQuery.Input.TransactionId != "" &&
		!requestQuery.IsTransactionEnd() &&
		!requestQuery.IsTransactionRollback() {
		transaction, err := request.databaseManager.Resources(
			databaseKey.DatabaseID,
			databaseKey.DatabaseBranchID,
		).TransactionManager().Get(string(requestQuery.Input.TransactionId))

		if err != nil {
			return JsonResponse(map[string]interface{}{
				"message": err.Error(),
			}, 500, nil,
			)
		}

		if accessKey.AccessKeyID != transaction.AccessKey.AccessKeyID {
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
