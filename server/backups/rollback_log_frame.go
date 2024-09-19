package backups

import (
	"encoding/binary"
	"fmt"
)

const RollbackFrameHeaderSize = 32

/*
The RollbackLogFrame is a data structure used to segment the rollback log into
frames of database updates. Each frame contains metadata about the changes made
to the database at a specific point in time and are used to efficiently manage
and restore database states during rollback operations.

When serialized the RollbackLogFrame data is stored in the following format:
| offset | size | description                       |
|--------|------|-----------------------------------|
| 0      | 4    | The identifier of the frame       |
| 4      | 4    | The committed state               |
| 8      | 8    | The offset of the data            |
| 16     | 8    | The size of the data              |
| 24     | 8    | The timestamp of the entry        |
*/
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
