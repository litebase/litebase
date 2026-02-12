package database

import (
	"bytes"
	"encoding/json"
	"sync"
)

const (
	// decoderBufferInitialSize is the initial capacity for decoder buffers
	decoderBufferInitialSize = 1024 // 1KB
	// decoderBufferMaxPoolSize prevents oversized buffers from being pooled
	decoderBufferMaxPoolSize = 64 * 1024 // 64KB
)

var staticDecoderPool *DecoderPool

type Decoder struct {
	Buffer      *bytes.Buffer
	JsonDecoder *json.Decoder
}

type DecoderPool struct {
	decoders *sync.Pool
}

func JsonDecoderPool() *DecoderPool {
	if staticDecoderPool == nil {
		staticDecoderPool = &DecoderPool{
			decoders: &sync.Pool{
				New: func() any {
					buffer := bytes.NewBuffer(make([]byte, 0, decoderBufferInitialSize))

					return &Decoder{
						Buffer:      buffer,
						JsonDecoder: json.NewDecoder(buffer),
					}
				},
			},
		}
	}

	return staticDecoderPool
}

func (ep *DecoderPool) Get() *Decoder {
	encoder := ep.decoders.Get().(*Decoder)
	encoder.Buffer.Reset()

	return encoder
}

func (ep *DecoderPool) Put(encoder *Decoder) {
	// Only return reasonably-sized buffers to pool
	if encoder.Buffer.Cap() <= decoderBufferMaxPoolSize {
		ep.decoders.Put(encoder)
	}
	// Oversized decoders are discarded and will be garbage collected
}
