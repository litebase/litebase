package backups

import (
	"encoding/binary"
	"fmt"

	"github.com/litebase/litebase/internal/utils"
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
	Committed uint32
	Offset    int64
	Size      int64
	Timestamp int64
}

// Deserialize a byte slice into a RollbackLogFrame.
func DeserializeRollbackLogFrame(data []byte) (RollbackLogFrame, error) {
	if len(data) < RollbackFrameHeaderSize {
		return RollbackLogFrame{}, fmt.Errorf("data length is less than %d bytes", RollbackFrameHeaderSize)
	}

	r := RollbackLogFrame{}

	id := RollbackLogIdentifier(binary.LittleEndian.Uint32(data[0:4]))

	if id != RollbackLogFrameID {
		return RollbackLogFrame{}, fmt.Errorf("this data is not a RollbackLogFrame expected: %d got: %d", RollbackLogFrameID, id)
	}

	r.Committed = binary.LittleEndian.Uint32(data[4:8])

	var err error

	r.Offset, err = utils.SafeUint64ToInt64(binary.LittleEndian.Uint64(data[8:16]))

	if err != nil {
		return RollbackLogFrame{}, err
	}

	r.Size, err = utils.SafeUint64ToInt64(binary.LittleEndian.Uint64(data[16:24]))

	if err != nil {
		return RollbackLogFrame{}, err
	}

	r.Timestamp, err = utils.SafeUint64ToInt64(binary.LittleEndian.Uint64(data[24:32]))

	if err != nil {
		return RollbackLogFrame{}, err
	}

	return r, nil
}

// Serialize the RollbackLogFrame into a byte slice.
func (r RollbackLogFrame) Serialize() ([]byte, error) {
	serialized := make([]byte, RollbackFrameHeaderSize)

	binary.LittleEndian.PutUint32(serialized[0:4], uint32(RollbackLogFrameID))
	binary.LittleEndian.PutUint32(serialized[4:8], r.Committed)

	uint64Offset, err := utils.SafeInt64ToUint64(r.Offset)

	if err != nil {
		return nil, err
	}

	binary.LittleEndian.PutUint64(serialized[8:16], uint64Offset)

	uint64Size, err := utils.SafeInt64ToUint64(r.Size)

	if err != nil {
		return nil, err
	}

	binary.LittleEndian.PutUint64(serialized[16:24], uint64Size)

	uint64Timestamp, err := utils.SafeInt64ToUint64(r.Timestamp)

	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint64(serialized[24:32], uint64Timestamp)

	return serialized, nil
}
