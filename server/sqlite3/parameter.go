package sqlite3

import (
	"sync"
)

type RawParameter struct {
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

var rawParameterPool = sync.Pool{
	New: func() interface{} {
		return &RawParameter{}
	},
}

var jsonParameterDecoderPool = JsonParameterDecoderPool()

type StatementParameter struct {
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

func (qp *StatementParameter) UnmarshalJSON(data []byte) error {
	raw := rawParameterPool.Get().(*RawParameter)
	defer rawParameterPool.Put(raw)

	decoder := jsonParameterDecoderPool.Get()
	defer jsonParameterDecoderPool.Put(decoder)

	decoder.Buffer.Write(data)

	// Reset the fields of the struct
	raw.Type = ""
	raw.Value = nil

	if err := decoder.JsonDecoder.Decode(raw); err != nil {
		return err
	}

	qp.Type = raw.Type
	qp.Value = raw.Value

	return nil
}
