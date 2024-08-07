package query

import (
	"encoding/json"
	"io"
	"litebase/server/sqlite3"
)

type Row interface {
	string | int | float64 | bool
}

type QueryResponse struct {
	Changes         int64              `json:"changes"`
	Columns         []string           `json:"columns"`
	ExecutionTime   float64            `json:"_executionTime"`
	Id              string             `json:"id"`
	LastInsertRowId int64              `json:"lastInsertRowID"`
	RowCount        int                `json:"rowCount"`
	Rows            [][]sqlite3.Column `json:"rows"`
}

type QueryJsonResponse struct {
	Status string        `json:"status"`
	Data   QueryResponse `json:"data"`
}

func (qr QueryResponse) ToJSON() ([]byte, error) {
	return json.Marshal(QueryJsonResponse{
		Status: "success",
		Data:   qr,
	})
}

func (qr QueryResponse) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"_executionTime":  qr.ExecutionTime,
			"changes":         qr.Changes,
			"id":              qr.Id,
			"lastInsertRowID": qr.LastInsertRowId,
			"columns":         qr.Columns,
			"rows":            qr.Rows,
			"rowCount":        qr.RowCount,
		},
	}
}

func (qr QueryResponse) WriteJson(w io.Writer) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", " ")

	return encoder.Encode(QueryJsonResponse{
		Status: "success",
		Data:   qr,
	})
}
