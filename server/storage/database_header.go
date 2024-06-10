package storage

import "encoding/binary"

type DatabaseHeader struct {
	HeaderString         []byte
	PageSize             uint16
	WriteVersion         uint8
	ReadVersion          uint8
	ReservedSpace        uint8
	MaxFraction          uint8
	MinFraction          uint8
	LeafFraction         uint8
	ChangeCounter        uint32
	TotalPages           uint32
	_                    uint32
	_                    uint32
	SchemaCookie         uint32
	SchemaFormat         uint32
	_                    uint32
	_                    uint32
	TextEncoding         uint32
	_                    uint32
	_                    uint32
	_                    uint32
	ReservedForExpansion []byte
	_                    uint32
	_                    uint32
}

func NewDatabaseHeader(data []byte) DatabaseHeader {
	header := DatabaseHeader{}

	header.HeaderString = data[0:16]
	header.PageSize = binary.LittleEndian.Uint16(data[16:18])
	header.WriteVersion = data[18]
	header.ReadVersion = data[19]
	header.ReservedSpace = data[20]
	header.MaxFraction = data[21]
	header.MinFraction = data[22]
	header.LeafFraction = data[23]
	header.ChangeCounter = binary.LittleEndian.Uint32(data[24:28])
	header.TotalPages = binary.LittleEndian.Uint32(data[28:32])
	header.SchemaCookie = binary.LittleEndian.Uint32(data[32:36])
	header.SchemaFormat = binary.LittleEndian.Uint32(data[36:40])
	header.TextEncoding = binary.LittleEndian.Uint32(data[56:60])
	header.ReservedForExpansion = data[60:92]

	return header

}
