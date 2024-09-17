package backups

import (
	"encoding/binary"
	"fmt"
)

const RollbackFrameHeaderSize = 32

type RollbackLogFrame struct {
	Committed int
	Offset    int64
	Size      int64
	Timestamp int64
}

func DeserializeRollbackLogFrame(data []byte) (RollbackLogFrame, error) {
	if len(data) < RollbackFrameHeaderSize {
		return RollbackLogFrame{}, fmt.Errorf("data length is less than %d bytes", RollbackFrameHeaderSize)
	}

	r := RollbackLogFrame{}

	id := RollbackLogIdentifier(binary.LittleEndian.Uint32(data[0:4]))

	if id != RollbackLogFrameID {
		return RollbackLogFrame{}, fmt.Errorf("this data is not a RollbackLogFrame expected: %d got: %d", RollbackLogFrameID, id)
	}

	r.Committed = int(binary.LittleEndian.Uint32(data[4:8]))
	r.Offset = int64(binary.LittleEndian.Uint64(data[8:16]))
	r.Size = int64(binary.LittleEndian.Uint64(data[16:24]))
	r.Timestamp = int64(binary.LittleEndian.Uint64(data[24:32]))

	return r, nil
}

func (r RollbackLogFrame) Serialize() ([]byte, error) {
	serialized := make([]byte, RollbackFrameHeaderSize)

	binary.LittleEndian.PutUint32(serialized[0:4], uint32(RollbackLogFrameID))
	binary.LittleEndian.PutUint32(serialized[4:8], uint32(r.Committed))
	binary.LittleEndian.PutUint64(serialized[8:16], uint64(r.Offset))
	binary.LittleEndian.PutUint64(serialized[16:24], uint64(r.Size))
	binary.LittleEndian.PutUint64(serialized[24:32], uint64(r.Timestamp))

	return serialized, nil
}
