package http

// TrasactionControllerStore creates a new transaction. This is effectively a
// call to begin a transaction.
func TrasactionControllerStore(request *Request) Response {
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

	transaction, err := request.databaseManager.Resources(
		databaseKey.DatabaseId,
		databaseKey.BranchId,
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
		Body: map[string]interface{}{
			"status":  "success",
			"message": "Transaction created successfully",
			"data": map[string]interface{}{
				"id":          transaction.Id,
				"database_id": databaseKey.DatabaseId,
				"branch_id":   databaseKey.BranchId,
				"created_at":  transaction.CreatedAt,
				"started_at":  transaction.StartedAt,
			},
		},
	}
}

// Destroying a transaction is where the transaction is rolled back.
func TransactionControllerDestroy(request *Request) Response {
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

	err = transaction.Rollback()

	if err != nil {
		return BadRequestResponse(err)
	}

	return Response{
		StatusCode: 200,
		Body: map[string]interface{}{
			"status":  "success",
			"message": "Transaction deleted successfully",
		},
	}
}
