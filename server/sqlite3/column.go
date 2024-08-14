package sqlite3

import (
	"encoding/json"
)

type Column struct {
	value interface{}
}

func (r Column) Blob() ([]byte, bool) {
	b, ok := r.value.([]byte)

	return b, ok
}

func (r Column) Bool() (bool, bool) {
	b, ok := r.value.(bool)

	return b, ok
}

func (r Column) Float64() (float64, bool) {
	f, ok := r.value.(float64)

	return f, ok
}

func (r Column) Int() (int, bool) {
	i, ok := r.value.(int)

	return i, ok
}

func (r Column) Int64() (int64, bool) {
	i, ok := r.value.(int64)

	return i, ok
}

func (r Column) String() (string, bool) {
	str, ok := r.value.(string)

	return str, ok
}

func NewColumn(v interface{}) Column {
	return Column{value: v}
}

// Implement the json.Marshaler interface
func (r Column) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.value)
}
