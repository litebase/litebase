package http

import (
	"errors"
)

var ErrInvalidAccessKey = errors.New("a valid access key is required to make this request")
var ErrInvalidAccessKeyResponse = BadRequestResponse(ErrInvalidAccessKey)
var ErrValidDatabaseKeyRequired = errors.New("a valid database is required to make this request")
var ErrValidDatabaseKeyRequiredResponse = BadRequestResponse(ErrValidDatabaseKeyRequired)
