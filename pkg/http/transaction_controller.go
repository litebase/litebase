package http

import (
	"errors"
	"fmt"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/database"
)

// TransactionControllerStore creates a new transaction. This is effectively a
// call to begin a transaction.
func TransactionControllerStore(request *Request) Response {
	databaseKey, errResponse := request.DatabaseKey()

	if !errResponse.IsEmpty() {
		return errResponse
	}

	requestToken := request.RequestToken("Authorization")

	if !requestToken.Valid() {
		return ErrInvalidAccessKeyResponse
	}

	accessKey := requestToken.AccessKey()

	if accessKey.AccessKeyID == "" {
		return ErrInvalidAccessKeyResponse
	}

	// Authorize the request
	err := request.Authorize(
		[]string{fmt.Sprintf("database:%s:branch:%s", databaseKey.DatabaseID, databaseKey.DatabaseBranchID)},
		[]auth.Privilege{auth.DatabasePrivilegeTransaction},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	transaction, err := request.databaseManager.Resources(
		databaseKey.DatabaseID,
		databaseKey.DatabaseBranchID,
	).TransactionManager().Create(
		request.cluster,
		request.databaseManager,
		databaseKey,
		accessKey,
	)

	if err != nil {
		return BadRequestResponse(err)
	}

	return Response{
		StatusCode: 200,
		Body: map[string]any{
			"status":  "success",
			"message": "Transaction created successfully",
			"data": map[string]any{
				"id":          transaction.ID,
				"database_id": databaseKey.DatabaseID,
				"branch_id":   databaseKey.DatabaseBranchID,
				"created_at":  transaction.CreatedAt,
				"started_at":  transaction.StartedAt,
			},
		},
	}
}

// Destroying a transaction is where the transaction is rolled back.
func TransactionControllerDestroy(request *Request) Response {
	databaseKey, errResponse := request.DatabaseKey()

	if !errResponse.IsEmpty() {
		return errResponse
	}

	requestToken := request.RequestToken("Authorization")

	if !requestToken.Valid() {
		return ErrInvalidAccessKeyResponse
	}

	accessKey := requestToken.AccessKey()

	if accessKey.AccessKeyID == "" {
		return ErrInvalidAccessKeyResponse
	}

	// Authorize the request
	err := request.Authorize(
		[]string{fmt.Sprintf("database:%s:branch:%s", databaseKey.DatabaseID, databaseKey.DatabaseBranchID)},
		[]auth.Privilege{auth.DatabasePrivilegeTransaction},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	transactionId := request.Param("id")
	transactionManager := request.databaseManager.Resources(
		databaseKey.DatabaseID,
		databaseKey.DatabaseBranchID,
	).TransactionManager()

	transaction, err := transactionManager.Get(transactionId)

	if err != nil {
		if err == database.ErrTransactionNotFound {
			return NotFoundResponse(errors.New("transaction not found"))
		}

		return BadRequestResponse(err)
	}

	defer transactionManager.Remove(transaction.ID)

	err = transaction.Rollback()

	if err != nil {
		return BadRequestResponse(err)
	}

	return Response{
		StatusCode: 200,
		Body: map[string]any{
			"status":  "success",
			"message": "Transaction deleted successfully",
		},
	}
}
