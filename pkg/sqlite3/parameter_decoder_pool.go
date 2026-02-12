package sqlite3

import (
	"bytes"
	"encoding/json"
	"sync"
)

const (
	// parameterDecoderBufferInitialSize is the initial capacity for parameter decoder buffers
	parameterDecoderBufferInitialSize = 1024 // 1KB
	// parameterDecoderBufferMaxPoolSize prevents oversized buffers from being pooled
	parameterDecoderBufferMaxPoolSize = 64 * 1024 // 64KB
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
				buffer := bytes.NewBuffer(make([]byte, 0, parameterDecoderBufferInitialSize))

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
	// Only return reasonably-sized buffers to pool
	if encoder.Buffer.Cap() <= parameterDecoderBufferMaxPoolSize {
		pdp.decoders.Put(encoder)
	}
	// Oversized decoders are discarded and will be garbage collected
}
