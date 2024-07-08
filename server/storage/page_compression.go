package storage

import (
	"log"

	"github.com/klauspost/compress/zstd"
)

var pageEncoder *zstd.Encoder
var pageDecoder *zstd.Decoder

func pgEncoder() *zstd.Encoder {
	if pageEncoder == nil {
		encoder, err := zstd.NewWriter(nil)

		if err != nil {
			log.Fatalln("Error creating zstd encoder", err)
		}

		pageEncoder = encoder
	}

	return pageEncoder
}

func pgDecoder() *zstd.Decoder {
	if pageDecoder == nil {
		decoder, err := zstd.NewReader(nil)

		if err != nil {
			log.Fatalln("Error creating zstd decoder", err)
		}

		pageDecoder = decoder
	}

	return pageDecoder
}
