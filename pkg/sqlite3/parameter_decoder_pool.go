package sqlite3

import (
	"bytes"
	"encoding/json"
	"sync"
)

type ParameterDecoder struct {
	Buffer      *bytes.Buffer
	JsonDecoder *json.Decoder
}

type ParameterDecoderPool struct {
	decoders *sync.Pool
}

func JsonParameterDecoderPool() *ParameterDecoderPool {
	return &ParameterDecoderPool{
		decoders: &sync.Pool{
			New: func() interface{} {
				buffer := bytes.NewBuffer(make([]byte, 1024))

				return &ParameterDecoder{
					Buffer:      buffer,
					JsonDecoder: json.NewDecoder(buffer),
				}
			},
		},
	}
}

func (pdp *ParameterDecoderPool) Get() *ParameterDecoder {
	encoder := pdp.decoders.Get().(*ParameterDecoder)
	encoder.Buffer.Reset()

	return encoder
}

func (pdp *ParameterDecoderPool) Put(encoder *ParameterDecoder) {
	pdp.decoders.Put(encoder)
}
