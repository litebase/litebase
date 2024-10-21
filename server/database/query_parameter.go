package database

import (
	"encoding/json"
	"fmt"
)

type QueryParameter struct {
	Value interface{}
}

func NewQueryParameter(value interface{}) QueryParameter {
	return QueryParameter{value}
}

// UnmarshalJSON implements the json.Unmarshaler interface for QueryParameter.
func (qp *QueryParameter) UnmarshalJSON(data []byte) error {
	var intVal int

	if err := json.Unmarshal(data, &intVal); err == nil {
		qp.Value = intVal
		return nil
	}

	var strVal string

	if err := json.Unmarshal(data, &strVal); err == nil {
		qp.Value = strVal
		return nil
	}

	return fmt.Errorf("unsupported parameter type")
}
