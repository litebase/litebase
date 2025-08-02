package http

import (
	"errors"
)

var ErrInvalidAccessKey = errors.New("a valid access key is required to make this request")
var ErrInvalidAccessKeyResponse = BadRequestResponse(ErrInvalidAccessKey)
var ErrValidDatabaseIdRequired = errors.New("a valid database ID is required")
var ErrValidDatabaseIdRequiredResponse = BadRequestResponse(ErrValidDatabaseIdRequired)
var ErrValidDatabaseNameRequired = errors.New("a valid database name is required")
var ErrValidDatabaseNameRequiredResponse = BadRequestResponse(ErrValidDatabaseNameRequired)
var ErrValidBranchIdRequired = errors.New("a valid branch ID is required")
var ErrValidBranchIdRequiredResponse = BadRequestResponse(ErrValidBranchIdRequired)
var ErrValidBranchNameRequired = errors.New("a valid branch name is required")
var ErrValidBranchNameRequiredResponse = BadRequestResponse(ErrValidBranchNameRequired)
var ErrValidDatabaseKeyRequired = errors.New("a valid database is required to make this request")
var ErrValidDatabaseKeyRequiredResponse = BadRequestResponse(ErrValidDatabaseKeyRequired)
var ErrInvalidInput = errors.New("invalid request input")
