package sqlite3

import (
	"encoding/json"
	"fmt"
)

type StatementParameter struct {
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

func (qp *StatementParameter) UnmarshalJSON(data []byte) error {
	var raw struct {
		Type  string      `json:"type"`
		Value interface{} `json:"value"`
	}

	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	qp.Type = raw.Type

	switch raw.Type {
	case "NULL":
		qp.Value = nil
	case "REAL":
		if v, ok := raw.Value.(float64); ok {
			qp.Value = v
		} else {
			return fmt.Errorf("invalid value for REAL type: %v", raw.Value)
		}
	case "INTEGER":
		if v, ok := raw.Value.(float64); ok {
			qp.Value = int(v)
		} else {
			return fmt.Errorf("invalid value for INTEGER type: %v", raw.Value)
		}
	case "TEXT":
		if v, ok := raw.Value.(string); ok {
			qp.Value = v
		} else {
			return fmt.Errorf("invalid value for TEXT type: %v", raw.Value)
		}
	case "BLOB":
		if v, ok := raw.Value.([]byte); ok {
			qp.Value = v
		} else {
			return fmt.Errorf("invalid value for BLOB type: %v", raw.Value)
		}
	default:
		return fmt.Errorf("unsupported type: %s", raw.Type)
	}

	return nil
}
