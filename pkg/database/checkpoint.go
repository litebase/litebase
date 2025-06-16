package database

import (
	"encoding/binary"

	"github.com/litebase/litebase/internal/utils"
)

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

	uint64CheckpointVersion, err := utils.SafeInt64ToUint64(CheckpointVersion)

	if err != nil {
		return nil
	}

	uint64BeginPageCount, err := utils.SafeInt64ToUint64(c.BeginPageCount)

	if err != nil {
		return nil
	}

	uint64LargestPageNumber, err := utils.SafeInt64ToUint64(c.LargestPageNumber)

	if err != nil {
		return nil
	}

	uint64Offset, err := utils.SafeInt64ToUint64(c.Offset)

	if err != nil {
		return nil
	}

	uint64Size, err := utils.SafeInt64ToUint64(c.Size)

	if err != nil {
		return nil
	}

	uint64Timestamp, err := utils.SafeInt64ToUint64(c.Timestamp)

	if err != nil {
		return nil
	}

	binary.LittleEndian.PutUint64(bytes[0:8], uint64CheckpointVersion)
	binary.LittleEndian.PutUint64(bytes[8:16], uint64BeginPageCount)
	binary.LittleEndian.PutUint64(bytes[16:24], uint64LargestPageNumber)
	binary.LittleEndian.PutUint64(bytes[24:32], uint64Offset)
	binary.LittleEndian.PutUint64(bytes[32:40], uint64Size)
	binary.LittleEndian.PutUint64(bytes[40:48], uint64Timestamp)

	return bytes
}

func DecodeCheckpoint(data []byte) *Checkpoint {
	beginPageCount, err := utils.SafeUint64ToInt64(binary.LittleEndian.Uint64(data[8:16]))

	if err != nil {
		beginPageCount = 0
	}

	largestPageNumber, err := utils.SafeUint64ToInt64(binary.LittleEndian.Uint64(data[16:24]))

	if err != nil {
		largestPageNumber = 0
	}

	offset, err := utils.SafeUint64ToInt64(binary.LittleEndian.Uint64(data[24:32]))

	if err != nil {
		offset = 0
	}

	size, err := utils.SafeUint64ToInt64(binary.LittleEndian.Uint64(data[32:40]))

	if err != nil {
		size = 0
	}

	timestamp, err := utils.SafeUint64ToInt64(binary.LittleEndian.Uint64(data[40:48]))

	if err != nil {
		timestamp = 0
	}

	return &Checkpoint{
		BeginPageCount:    beginPageCount,
		LargestPageNumber: largestPageNumber,
		Offset:            offset,
		Size:              size,
		Timestamp:         timestamp,
	}
}
