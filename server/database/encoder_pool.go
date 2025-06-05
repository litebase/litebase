package database

import (
	"bytes"
	"encoding/json"
	"sync"
)

var staticEncoderPool *EncoderPool

type Encoder struct {
	Buffer      *bytes.Buffer
	JsonEncoder *json.Encoder
}

type EncoderPool struct {
	encoders *sync.Pool
}

func JsonEncoderPool() *EncoderPool {
	if staticEncoderPool == nil {
		staticEncoderPool = &EncoderPool{
			encoders: &sync.Pool{
				New: func() any {
					buffer := bytes.NewBuffer(make([]byte, 1024))

					return &Encoder{
						Buffer:      buffer,
						JsonEncoder: json.NewEncoder(buffer),
					}
				},
			},
		}
	}

	return staticEncoderPool
}

func (ep *EncoderPool) Get() *Encoder {
	encoder := ep.encoders.Get().(*Encoder)
	encoder.Buffer.Reset()

	return encoder
}

func (ep *EncoderPool) Put(encoder *Encoder) {
	ep.encoders.Put(encoder)
}
