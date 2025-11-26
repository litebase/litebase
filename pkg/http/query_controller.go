package http

import (
	"context"
	"errors"
	"fmt"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/sqlite3"
	"golang.org/x/exp/slog"
)

type QueryRequest struct {
	Queries []*database.QueryInput `json:"queries" validate:"required,min=1,dive"`
}

type QueryResponse struct {
	Changes         int64                      `json:"changes"`
	Columns         []sqlite3.ColumnDefinition `json:"columns"`
	ID              string                     `json:"id"`
	Latency         float64                    `json:"latency"`
	LastInsertRowId int64                      `json:"lastInsertRowId"`
	RowCount        int                        `json:"rowCount"`
	Rows            [][]*sqlite3.ColumnValue   `json:"rows"`
	TransactionID   string                     `json:"transactionId,omitempty"`
}

// Array of query responses for one or more queries
type QueryControllerStoreResponse []QueryResponse

// Execute one or more SQL queries against the specified database and branch.
func QueryControllerStore(ctx context.Context, request *Request) Response {
	databaseKey, errResponse := request.DatabaseKey()

	if !errResponse.IsEmpty() {
		return errResponse
	}

	credential := request.Credential()

	if !credential.Valid() {
		return ErrInvalidCredentialResponse
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

	req, ok := queries.(*QueryRequest)

	if !ok {
		return ServerErrorResponse(errors.New("invalid request format"))
	}

	// Validate the input
	validationErrors := request.Validate(queries, map[string]string{
		"queries.*.id.required":                        "The query ID field is required",
		"queries.*.parameters.required":                "The parameters field is required",
		"queries.*.parameters.*.type.required":         "The parameter type field is required",
		"queries.*.parameters.*.type.oneof":            "The parameter type field must be one of the allowed values",
		"queries.*.parameters.*.value.required":        "The parameter value field is required",
		"queries.*.parameters.*.value.required_unless": "The parameter value field is required unless the type is NULL",
		"queries.*.statement.required":                 "The SQL statement field is required",
		"queries.*.transaction_id.required":            "The transaction ID field is required",
	})

	if validationErrors != nil {
		return ValidationErrorResponse(validationErrors)
	}

	var responses QueryControllerStoreResponse

	for _, query := range req.Queries {
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

			if transaction.Credential != nil && credential.CredentialID != transaction.Credential.CredentialID {
				return ErrInvalidCredentialResponse
			}

			err = transaction.ResolveQuery(requestQuery, response)

			if err != nil {
				// TODO: BadRequestResponse instead
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
				// TODO: BadRequestResponse instead
				return ServerErrorResponse(err)
			}
		}

		responses = append(responses, QueryResponse{
			Changes:         response.Changes(),
			Columns:         adjustColumnTypesForLargeIntegers(response.Columns(), response.Rows()),
			ID:              response.Id(),
			Latency:         response.Latency(),
			LastInsertRowId: response.LastInsertRowId(),
			RowCount:        response.RowCount(),
			Rows:            response.RowValues(),
			TransactionID:   response.TransactionID(),
		})
	}

	return SuccessResponse(
		"Queries executed successfully",
		responses,
		200,
	)
}

// adjustColumnTypesForLargeIntegers scans the result rows and updates column types
// to INTEGER_LARGE (6) for any INTEGER columns that contain values beyond JavaScript's
// safe integer range (±2^53-1). This allows clients to know which integer values
// are encoded as strings in the JSON response.
func adjustColumnTypesForLargeIntegers(columns []sqlite3.ColumnDefinition, rows [][]*sqlite3.Column) []sqlite3.ColumnDefinition {
	if len(rows) == 0 || len(columns) == 0 {
		return columns
	}

	const maxSafeInteger = 9007199254740991  // 2^53 - 1
	const minSafeInteger = -9007199254740991 // -(2^53 - 1)

	// Track which columns have large integers
	hasLargeInt := make([]bool, len(columns))

	// Scan all rows to find large integers
	for _, row := range rows {
		for colIdx, col := range row {
			if colIdx >= len(columns) {
				continue
			}

			// Only check INTEGER columns
			if columns[colIdx].ColumnType == sqlite3.ColumnTypeInteger {
				intValue := col.Int64()

				if intValue > maxSafeInteger || intValue < minSafeInteger {
					hasLargeInt[colIdx] = true
				}
			}
		}
	}

	// Create a new column definitions slice with updated types
	adjustedColumns := make([]sqlite3.ColumnDefinition, len(columns))
	copy(adjustedColumns, columns)

	for i, hasLarge := range hasLargeInt {
		if hasLarge {
			adjustedColumns[i].ColumnType = sqlite3.ColumnTypeIntegerLarge
		}
	}

	return adjustedColumns
}
