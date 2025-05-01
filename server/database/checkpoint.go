package database

import "encoding/binary"

const CheckpointVersion = 1

type Checkpoint struct {
	BeginPageCount    int64
	LargestPageNumber int64
	Offset            int64
	Size              int64
	Timestamp         int64
}

func (c *Checkpoint) Encode() []byte {
	bytes := make([]byte, 48)

	binary.LittleEndian.PutUint64(bytes[0:8], uint64(CheckpointVersion))
	binary.LittleEndian.PutUint64(bytes[8:16], uint64(c.BeginPageCount))
	binary.LittleEndian.PutUint64(bytes[16:24], uint64(c.LargestPageNumber))
	binary.LittleEndian.PutUint64(bytes[24:32], uint64(c.Offset))
	binary.LittleEndian.PutUint64(bytes[32:40], uint64(c.Size))
	binary.LittleEndian.PutUint64(bytes[40:48], uint64(c.Timestamp))

	return bytes
}

func DecodeCheckpoint(data []byte) *Checkpoint {
	return &Checkpoint{
		BeginPageCount:    int64(binary.LittleEndian.Uint64(data[8:16])),
		LargestPageNumber: int64(binary.LittleEndian.Uint64(data[16:24])),
		Offset:            int64(binary.LittleEndian.Uint64(data[24:32])),
		Size:              int64(binary.LittleEndian.Uint64(data[32:40])),
		Timestamp:         int64(binary.LittleEndian.Uint64(data[40:48])),
	}
}
