package database

import "litebasedb/runtime/app/sqlite3"

type DatabaseOperator struct {
	inTransaction  bool
	isWriting      bool
	isTransmitting bool
}

var Operator = DatabaseOperator{
	inTransaction: false,
	isWriting:     false,
}

func (o *DatabaseOperator) Monitor(isRead bool, callback func() (sqlite3.Result, error)) (sqlite3.Result, error) {
	o.isWriting = !isRead
	result, err := callback()
	o.isWriting = false

	return result, err
}

func (o *DatabaseOperator) InTransaction() bool {
	return o.inTransaction
}

func (o *DatabaseOperator) IsWriting() bool {
	return o.isWriting
}

func (o *DatabaseOperator) Record() {

}

func (o *DatabaseOperator) Transmit() {
	o.isTransmitting = true
	WAL.CheckPoint()
	o.isTransmitting = false
}

func (o *DatabaseOperator) Transmitting() bool {
	return o.isTransmitting
}
