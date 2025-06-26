package backups

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"sync"
	"time"

	internalStorage "github.com/litebase/litebase/internal/storage"
	"github.com/litebase/litebase/internal/utils"
	"github.com/litebase/litebase/pkg/file"
	"github.com/litebase/litebase/pkg/storage"
)

type Snapshot struct {
	// The UUID of the branch the snapshot is for.
	BranchId string `json:"branch_id"`

	// The UUID of the database the snapshot is for.
	DatabaseId string `json:"database_id"`

	// The file to write the snapshot log to.
	File internalStorage.File `json:"-"`

	// The last time the snapshot was accessed. This timestamp is used for
	// cleanup purposes.
	LastAccessedAt int64 `json:"-"`

	// A mutex to lock the snapshot for concurrent access. This is especially
	// necessary when writing to the snapshot log file while backups are being
	// processed at the same time.
	mutex sync.Mutex

	// A list of restore points for the snapshot.
	RestorePoints SnapshotRestorePoints `json:"restore_points,omitempty"`

	// The UTC start of the day of the snapshot.
	Timestamp int64 `json:"timestamp"`

	tieredFS *storage.FileSystem
}

type SnapshotRestorePoints struct {
	Data  []int64 `json:"data"`
	Start int64   `json:"start"`
	End   int64   `json:"end"`
	Total int     `json:"total"`
}

type RestorePoint struct {
	Timestamp int64
	PageCount int64
}

// Create a new instance of a snapshot.
func NewSnapshot(tieredFS *storage.FileSystem, databaseId string, branchId string, dayTimestamp, timestamp int64) *Snapshot {
	return &Snapshot{
		BranchId:       branchId,
		DatabaseId:     databaseId,
		LastAccessedAt: time.Now().UTC().UnixNano(),
		RestorePoints: SnapshotRestorePoints{
			Data:  []int64{},
			Start: timestamp,
			End:   timestamp,
			Total: 0,
		},
		Timestamp: dayTimestamp,
		tieredFS:  tieredFS,
	}
}

// Close the snapshot file.
func (s *Snapshot) Close() error {
	if s.File != nil {
		return s.File.Close()
	}

	return nil
}

// Return the path to the snapshot log file for a database.
func GetSnapshotPath(databaseId string, branchId string, timestamp int64) string {
	return fmt.Sprintf(
		"%s/%d",
		file.GetDatabaseSnapshotDirectory(databaseId, branchId),
		timestamp,
	)
}

// Get a specific restore point from the snapshot file.
func (s *Snapshot) GetRestorePoint(timestamp int64) (RestorePoint, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.File == nil {
		err := s.openFile()

		if err != nil {
			return RestorePoint{}, err
		}
	}

	_, err := s.File.Seek(0, io.SeekStart)

	if err != nil {
		log.Println("Error seeking to start of snapshot file", err)
		return RestorePoint{}, err
	}

	var restorePoint RestorePoint

	for {
		data := make([]byte, 12) // 8 bytes for timestamp, 4 bytes for page count

		_, err := s.File.Read(data)

		if err != nil {
			break
		}

		t, err := utils.SafeUint64ToInt64(binary.LittleEndian.Uint64(data[0:8]))

		if err != nil {
			return RestorePoint{}, err
		}

		if int64(t) == timestamp {
			restorePoint = RestorePoint{
				Timestamp: t,
				PageCount: int64(binary.LittleEndian.Uint32(data[8:12])),
			}

			break
		}
	}

	return restorePoint, nil
}

// Determine if the snapshot is empty.
func (s *Snapshot) IsEmpty() bool {
	return s.Timestamp == 0
}

// Load the data of the snapshot that includes the restore points. These are
// logged in chronological order in the snapshot log file.
func (s *Snapshot) Load() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.File == nil {
		err := s.openFile()

		if err != nil {
			return err
		}
	}

	s.RestorePoints.Data = []int64{}
	s.RestorePoints.Start = 0
	s.RestorePoints.End = 0
	s.RestorePoints.Total = 0

	_, err := s.File.Seek(0, io.SeekStart)

	if err != nil {
		log.Println("Error seeking to start of snapshot file", err)

		return err
	}

	for {
		data := make([]byte, 12)

		_, err := s.File.Read(data)

		if err != nil {
			if err == io.EOF {
				// End of file reached, this is expected
				break
			}

			slog.Error("Error reading snapshot file", "error", err)

			// An error occurred while reading the file
			return err
		}

		t, err := utils.SafeUint64ToInt64(binary.LittleEndian.Uint64(data[0:8]))

		if err != nil {
			return err
		}

		// Get the start of the day of the timestamp
		s.RestorePoints.Data = append(s.RestorePoints.Data, t)

		if s.RestorePoints.Start == 0 || t < s.RestorePoints.Start {
			s.RestorePoints.Start = t
		}

		s.RestorePoints.End = t
		s.RestorePoints.Total++
	}

	return nil
}

// Write a new restore point to the snapshot log file. This is used to log
// the state of the database at a specific point in time in chronological order.
func (s *Snapshot) Log(timestamp, pageCount int64) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.File == nil {
		err := s.openFile()

		if err != nil {
			return err
		}
	}

	_, err := s.File.Seek(0, io.SeekEnd)

	if err != nil {
		return err
	}

	data := make([]byte, 12) // 8 bytes for timestamp, 4 bytes for page count

	uint64Timestamp, err := utils.SafeInt64ToUint64(timestamp)

	if err != nil {
		slog.Error("Error converting timestamp to uint64:", "error", err)
		return err
	}

	binary.LittleEndian.PutUint64(data[0:8], uint64Timestamp)

	uint32PageCount, err := utils.SafeInt64ToUint32(pageCount)

	if err != nil {
		return err
	}

	binary.LittleEndian.PutUint32(data[8:12], uint32PageCount)

	_, err = s.File.Write(data)

	if err != nil {
		return err
	}

	// Ensure data is flushed to disk immediately
	return s.File.Sync()
}

func (s *Snapshot) openFile() error {
openFile:
	snapshotFile, err := s.tieredFS.OpenFile(
		GetSnapshotPath(s.DatabaseId, s.BranchId, s.Timestamp),
		SNAPSHOT_LOG_FLAGS,
		0600,
	)

	if err != nil {
		if os.IsNotExist(err) {
			err := s.tieredFS.MkdirAll(fmt.Sprintf("%s/logs/snapshots", file.GetDatabaseFileBaseDir(s.DatabaseId, s.BranchId)), 0750)

			if err != nil {
				return err
			}

			goto openFile
		} else {
			log.Println("Error opening snapshot file", err)
			return err
		}
	}

	s.File = snapshotFile

	return nil
}
