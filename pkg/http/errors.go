package http

import (
	"errors"
)

var ErrInvalidAccessKey = errors.New("a valid access key is required to make this request")
var ErrInvalidAccessKeyResponse = BadRequestResponse(ErrInvalidAccessKey)
var ErrValidDatabaseIdRequired = errors.New("a valid database ID is required")
var ErrValidDatabaseIdRequiredResponse = BadRequestResponse(ErrValidDatabaseIdRequired)
var ErrValidBranchIdRequired = errors.New("a valid branch ID is required")
var ErrValidBranchIdRequiredResponse = BadRequestResponse(ErrValidBranchIdRequired)
var ErrValidDatabaseKeyRequired = errors.New("a valid database is required to make this request")
var ErrValidDatabaseKeyRequiredResponse = BadRequestResponse(ErrValidDatabaseKeyRequired)
var ErrInvalidInput = errors.New("invalid request input")
