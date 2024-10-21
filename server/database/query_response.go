package database

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
	Id              string             `json:"id"`
	Latency         float64            `json:"latency"`
	LastInsertRowId int64              `json:"lastInsertRowID"`
	RowCount        int                `json:"rowCount"`
	Rows            [][]sqlite3.Column `json:"rows"`
}

type QueryJsonResponse struct {
	Status string         `json:"status"`
	Data   *QueryResponse `json:"data"`
}

func (qr *QueryResponse) JsonResponse() QueryJsonResponse {
	return QueryJsonResponse{
		Status: "success",
		Data:   qr,
	}
}

func (qr *QueryResponse) Reset() {
	qr.Changes = 0
	qr.Columns = nil
	qr.Id = ""
	qr.Latency = 0
	qr.LastInsertRowId = 0
	qr.RowCount = 0
	qr.Rows = [][]sqlite3.Column{}
}

func (qr *QueryResponse) ToJSON() ([]byte, error) {
	return json.Marshal(QueryJsonResponse{
		Status: "success",
		Data:   qr,
	})
}

func (qr QueryResponse) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"changes":         qr.Changes,
			"id":              qr.Id,
			"latency":         qr.Latency,
			"lastInsertRowID": qr.LastInsertRowId,
			"columns":         qr.Columns,
			"rows":            qr.Rows,
			"rowCount":        qr.RowCount,
		},
	}
}

func (qr *QueryResponse) WriteJson(w io.Writer) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", " ")

	return encoder.Encode(QueryJsonResponse{
		Status: "success",
		Data:   qr,
	})
}
