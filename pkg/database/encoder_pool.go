package database

import (
	"bytes"
	"encoding/json"
	"sync"
)

const (
	// encoderBufferInitialSize is the initial capacity for encoder buffers
	encoderBufferInitialSize = 1024 // 1KB
	// encoderBufferMaxPoolSize prevents oversized buffers from being pooled
	encoderBufferMaxPoolSize = 64 * 1024 // 64KB
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
					buffer := bytes.NewBuffer(make([]byte, 0, encoderBufferInitialSize))

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
	// Only return reasonably-sized buffers to pool
	if encoder.Buffer.Cap() <= encoderBufferMaxPoolSize {
		ep.encoders.Put(encoder)
	}
	// Oversized encoders are discarded and will be garbage collected
}
