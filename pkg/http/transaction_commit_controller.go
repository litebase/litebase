package http

import (
	"fmt"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/database"
)

func TransactionCommitController(request *Request) Response {
	databaseKey, errResponse := request.DatabaseKey()

	if !errResponse.IsEmpty() {
		return errResponse
	}

	if databaseKey == nil {
		return ErrValidDatabaseKeyRequiredResponse
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
			return NotFoundResponse(err)
		}

		return BadRequestResponse(err)
	}

	defer transactionManager.Remove(transaction.ID)

	err = transaction.Commit()

	if err != nil {
		return BadRequestResponse(err)
	}

	return Response{
		StatusCode: 200,
		Body: map[string]interface{}{
			"status":  "success",
			"message": "Transaction committed successfully",
		},
	}
}
