package database

type DatabaseHeader struct {
	HeaderString         [16]byte
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
	ReservedForExpansion [20]byte
	_                    uint32
	_                    uint32
}
