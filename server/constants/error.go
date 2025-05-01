package constants

import "fmt"

type ErrorCode int

const (
	ErrSnapshotConflict ErrorCode = 11001
)

type ServerError interface {
	Code() ErrorCode
	Error() string
}

var ServerErrors = map[ErrorCode]ServerError{
	ErrSnapshotConflict: ErrorSnapshotConflict{},
}

func ErrorFromCode(code int) error {
	return ServerErrors[ErrorCode(code)]
}

type ErrorSnapshotConflict struct{}

func (e ErrorSnapshotConflict) Code() ErrorCode {
	return ErrSnapshotConflict
}

func (e ErrorSnapshotConflict) Error() string {
	return fmt.Sprintf("Litebase Error[%d]: snapshot isolation conflict", e.Code())
}
