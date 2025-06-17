package http

func TransactionCommitController(request *Request) Response {
	databaseKey := request.DatabaseKey()

	if databaseKey == nil {
		return ErrValidDatabaseKeyRequiredResponse
	}

	requestToken := request.RequestToken("Authorization")

	if !requestToken.Valid() {
		return ErrInvalidAccessKeyResponse
	}

	accessKey := requestToken.AccessKey()

	if accessKey.AccessKeyId == "" {
		return ErrInvalidAccessKeyResponse
	}

	transactionId := request.Param("id")
	transactionManager := request.databaseManager.Resources(
		databaseKey.DatabaseId,
		databaseKey.BranchId,
	).TransactionManager()

	transaction, err := transactionManager.Get(transactionId)

	if err != nil {
		return BadRequestResponse(err)
	}

	defer transactionManager.Remove(transaction.Id)

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
