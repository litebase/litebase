package http

import (
	"fmt"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/database"
	"golang.org/x/exp/slog"
)

type QueryRequest struct {
	Queries []*database.QueryInput `json:"queries" validate:"required,min=1,dive"`
}

func QueryController(request *Request) Response {
	databaseKey, errResponse := request.DatabaseKey()

	if !errResponse.IsEmpty() {
		return errResponse
	}

	credential := request.Credential()

	if !credential.Valid() {
		return ErrInvalidAccessKeyResponse
	}

	accessKey := credential.AccessKey()

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

	queries, err := request.Input(&QueryRequest{})

	if err != nil {
		slog.Error("failed to parse input", "error", err.Error())

		return BadRequestResponse(ErrInvalidInput)
	}

	// Validate the input
	validationErrors := request.Validate(queries, map[string]string{
		"queries.*.id.required":                        "The query ID field is required.",
		"queries.*.parameters.required":                "The parameters field is required.",
		"queries.*.parameters.*.type.required":         "The parameter type field is required.",
		"queries.*.parameters.*.type.oneof":            "The parameter type field must be one of the allowed values.",
		"queries.*.parameters.*.value.required":        "The parameter value field is required.",
		"queries.*.parameters.*.value.required_unless": "The parameter value field is required unless the type is NULL.",
		"queries.*.statement.required":                 "The SQL statement field is required.",
		"queries.*.transaction_id.required":            "The transaction ID field is required.",
	})

	if validationErrors != nil {
		return ValidationErrorResponse(validationErrors)
	}

	responses := []map[string]any{}

	for _, query := range queries.(*QueryRequest).Queries {
		requestQuery := database.GetQuery(
			request.cluster,
			request.databaseManager,
			request.logManager,
			databaseKey,
			credential,
			query,
		)

		defer database.PutQuery(requestQuery)

		response := &database.QueryResponse{}
		resources := request.databaseManager.Resources(
			databaseKey.DatabaseID,
			databaseKey.DatabaseBranchID,
		)

		if requestQuery.Input.TransactionID != "" {
			transaction, err := resources.TransactionManager().Get(string(requestQuery.Input.TransactionID))

			if err != nil {
				return ServerErrorResponse(err)
			}

			if credential.CredentialID != transaction.Credential.CredentialID {
				return ErrInvalidAccessKeyResponse
			}

			err = transaction.ResolveQuery(requestQuery, response)

			if err != nil {
				return ServerErrorResponse(err)
			}

			// Check if this query ends the transaction (COMMIT or ROLLBACK)
			if requestQuery.IsTransactionEnd() || requestQuery.IsTransactionRollback() {
				// Transaction is automatically closed by ResolveQuery, just remove it from manager
				resources.TransactionManager().Remove(string(requestQuery.Input.TransactionID))
			}
		} else {
			_, err = requestQuery.Resolve(response)

			if err != nil {
				return ServerErrorResponse(err)
			}
		}

		responses = append(responses, response.ToMap())
	}

	return Response{
		StatusCode: 200,
		Body: map[string]any{
			"status": "success",
			"data":   responses,
		},
	}
}
