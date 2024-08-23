package vfs_test

import (
	"fmt"
	"litebase/internal/config"
	"litebase/internal/test"
	"litebase/server/vfs"
	"os"
	"testing"
)

func TestNewDataRange(t *testing.T) {
	test.Run(t, func() {
		dataRange := vfs.CNewDataRange(fmt.Sprintf("%s/TEST_DATA_RANGE", config.Get().DataPath), 1, 4096)

		if dataRange == nil {
			t.Errorf("NewDataRange() failed, expected not nil, got nil")
		}
	})
}

func TestDataRangeReadAt(t *testing.T) {
	test.Run(t, func() {
		dataRange := vfs.CNewDataRange(fmt.Sprintf("%s/TEST_DATA_RANGE", config.Get().DataPath), 1, 4096)

		if dataRange == nil {
			t.Errorf("NewDataRange() failed, expected not nil, got nil")
		}
	})
}

func TestDataRangeWriteAt(t *testing.T) {
	test.Run(t, func() {
		var dataSize int64 = 4096
		var entryCount int = 1024

		expectedSize := (dataSize * int64(entryCount))

		dataRange := vfs.CNewDataRange(fmt.Sprintf("%s/TEST_DATA_RANGE", config.Get().DataPath), 1, 4096)

		if dataRange == nil {
			t.Errorf("NewDataRange() failed, expected not nil, got nil")
		}

		for i := 0; i < entryCount; i++ {
			// Fill the data with the value i of the size of dataSize
			data := make([]byte, dataSize)

			for j := 0; j < len(data); j++ {
				data[j] = byte(i)
			}

			offset := int64(i * 4096)

			rc := vfs.CDataRangeWriteAt(dataRange, data, offset)

			if rc != 0 {
				t.Errorf("WriteAt() failed")
			}
		}

		info, err := os.Stat(fmt.Sprintf("%s/TEST_DATA_RANGE/%010d", config.Get().DataPath, 1))

		if err != nil {
			t.Errorf("Failed to get file info, expected nil, got %s", err)
		}

		if info.Size() != expectedSize {
			t.Errorf("WriteAt() failed, expected %d, got %d", expectedSize, info.Size())
		}

		// Verify the data in the range index
		file, err := os.Open(fmt.Sprintf("%s/TEST_DATA_RANGE/%010d", config.Get().DataPath, 1))

		if err != nil {
			t.Errorf("Failed to open file, expected nil, got %s", err)
		}

		// Read the index entries that should match the expected offsets
		for i := 0; i < entryCount; i++ {
			buffer := make([]byte, dataSize)

			readBytes, err := file.ReadAt(buffer, int64(i*4096))

			if err != nil {
				t.Errorf("Failed to read file, expected nil, got %s", err)
			}

			if readBytes != int(dataSize) {
				t.Errorf("ReadAt() failed, expected %d, got %d", dataSize, readBytes)
			}

			for j := 0; j < len(buffer); j++ {
				if buffer[j] != byte(i) {
					t.Errorf("ReadAt() data does not match")
				}
			}
		}
	})
}

func TestDataRangeClose(t *testing.T) {
	test.Run(t, func() {
		dataRange := vfs.CNewDataRange(fmt.Sprintf("%s/TEST_DATA_RANGE", config.Get().DataPath), 1, 4096)

		if dataRange == nil {
			t.Errorf("NewDataRange() failed, expected not nil, got nil")
		}

		rc := vfs.CDataRangeClose(dataRange)

		if rc != 0 {
			t.Errorf("Close() failed, expected 0, got %d", rc)
		}
	})
}

func TestDataRangeRemove(t *testing.T) {
	test.Run(t, func() {
		dataRange := vfs.CNewDataRange(fmt.Sprintf("%s/TEST_DATA_RANGE", config.Get().DataPath), 1, 4096)

		if dataRange == nil {
			t.Errorf("NewDataRange() failed, expected not nil, got nil")
		}

		rc := vfs.CDataRangeRemove(dataRange)

		if rc != 0 {
			t.Errorf("Remove() failed, expected 0, got %d", rc)
		}

		// Verify the data range is removed
		_, err := os.Stat(fmt.Sprintf("%s/TEST_DATA_RANGE/%010d", config.Get().DataPath, 1))

		if err == nil {
			t.Errorf("Remove() failed, expected error, got nil")
		}
	})
}

func TestDataRangeSize(t *testing.T) {
	test.Run(t, func() {
		dataRange := vfs.CNewDataRange(fmt.Sprintf("%s/TEST_DATA_RANGE", config.Get().DataPath), 1, 4096)

		if dataRange == nil {
			t.Errorf("NewDataRange() failed, expected not nil, got nil")
		}

		size := vfs.CDataRangeSize(dataRange)

		if size != 0 {
			t.Errorf("Size() failed, expected 0, got %d", size)
		}

		// Write some data to the data range
		rc := vfs.CDataRangeWriteAt(dataRange, []byte("test"), 0)

		if rc != 0 {
			t.Errorf("WriteAt() failed")
		}

		size = vfs.CDataRangeSize(dataRange)

		if size != 4096 {
			t.Errorf("Size() failed, expected 4096, got %d", size)
		}
	})
}

func TestDataRangeTruncate(t *testing.T) {
	test.Run(t, func() {
		dataRange := vfs.CNewDataRange(fmt.Sprintf("%s/TEST_DATA_RANGE", config.Get().DataPath), 1, 4096)

		if dataRange == nil {
			t.Errorf("NewDataRange() failed, expected not nil, got nil")
		}

		rc := vfs.CDataRangeWriteAt(dataRange, []byte("test"), 4096)

		if rc != 0 {
			t.Errorf("WriteAt() failed")
		}

		rc = vfs.CDataRangeTruncate(dataRange, 1024)

		if rc != 0 {
			t.Errorf("Truncate() failed, expected 0, got %d", rc)
		}
	})
}
