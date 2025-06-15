package database

import (
	"bytes"
	"encoding/json"
	"sync"
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
					buffer := bytes.NewBuffer(make([]byte, 1024))

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
	ep.decoders.Put(encoder)
}
