package storage_test

import (
	"fmt"
	"litebase/internal/config"
	"litebase/internal/test"
	"litebase/server/storage"
	"testing"
)

func TestNewDataRange(t *testing.T) {
	test.Run(t, func() {

		dataRange, err := storage.NewDataRange(storage.LocalFS(), "TEST_DATA_RANGE", 1, 4096)

		if err != nil {
			t.Errorf("NewDataRange() failed, expected nil, got %s", err)
		}

		if dataRange == nil {
			t.Errorf("NewDataRange() failed, expected not nil, got nil")
		}
	})
}

func TestDataRangeReadAt(t *testing.T) {
	test.Run(t, func() {
		dataRange, err := storage.NewDataRange(storage.LocalFS(), "TEST_DATA_RANGE", 1, 4096)

		if err != nil {
			t.Errorf("NewDataRange() failed, expected nil, got %s", err)
		}

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

		dataRange, err := storage.NewDataRange(storage.LocalFS(), "TEST_DATA_RANGE", 1, 4096)

		if err != nil {
			t.Errorf("NewDataRange() failed, expected nil, got %s", err)
		}

		if dataRange == nil {
			t.Errorf("NewDataRange() failed, expected not nil, got nil")
		}

		for i := 0; i < entryCount; i++ {
			// Fill the data with the value i of the size of dataSize
			data := make([]byte, dataSize)

			for j := 0; j < len(data); j++ {
				data[j] = byte(i)
			}

			var pageNumber int64 = int64(i + 1)

			n, err := dataRange.WriteAt(data, pageNumber)

			if err != nil {
				t.Errorf("WriteAt() failed, expected nil, got %s", err)
			}

			if n != int(dataSize) {
				t.Errorf("WriteAt() failed, expected %d, got %d", dataSize, n)
			}
		}

		info, err := storage.LocalFS().Stat(fmt.Sprintf("TEST_DATA_RANGE/%010d/", 1))

		if err != nil {
			t.Errorf("Failed to get file info, expected nil, got %s", err)
		}

		if info.Size() != expectedSize {
			t.Errorf("WriteAt() failed, expected %d, got %d", expectedSize, info.Size())
		}

		// // Verify the data in the range index
		// file, err := storage.LocalFS().Open(fmt.Sprintf("TEST_DATA_RANGE/%010d", 1))

		// if err != nil {
		// 	t.Errorf("Failed to open file, expected nil, got %s", err)
		// }

		// // Read the index entries that should match the expected offsets
		// for i := 0; i < entryCount; i++ {
		// 	buffer := make([]byte, dataSize)

		// 	readBytes, err := file.ReadAt(buffer, int64(i*4096))

		// 	if err != nil {
		// 		t.Errorf("Failed to read file, expected nil, got %s", err)
		// 	}

		// 	if readBytes != int(dataSize) {
		// 		t.Errorf("ReadAt() failed, expected %d, got %d", dataSize, readBytes)
		// 	}

		// 	for j := 0; j < len(buffer); j++ {
		// 		if buffer[j] != byte(i) {
		// 			t.Fatalf("ReadAt() data does not match")

		// 			break
		// 		}
		// 	}
		// }
	})
}

func TestDataRangeClose(t *testing.T) {
	test.Run(t, func() {
		dataRange, err := storage.NewDataRange(storage.LocalFS(), "TEST_DATA_RANGE", 1, 4096)

		if err != nil {
			t.Errorf("NewDataRange() failed, expected nil, got %s", err)
		}

		if dataRange == nil {
			t.Errorf("NewDataRange() failed, expected not nil, got nil")
		}

		err = dataRange.Close()

		if err != nil {
			t.Errorf("Close() failed, expected nil, got %s", err)
		}
	})
}

func TestDataRangeRemove(t *testing.T) {
	test.Run(t, func() {
		dataRange, err := storage.NewDataRange(storage.LocalFS(), "TEST_DATA_RANGE", 1, 4096)

		if err != nil {
			t.Errorf("NewDataRange() failed, expected nil, got %s", err)
		}

		if dataRange == nil {
			t.Errorf("NewDataRange() failed, expected not nil, got nil")
		}

		err = dataRange.Delete()

		if err != nil {
			t.Errorf("Remove() failed, expected nil, got %s", err)
		}

		// Verify the data range is removed
		_, err = storage.LocalFS().Stat(fmt.Sprintf("%s/TEST_DATA_RANGE/%010d/", config.Get().DataPath, 1))

		if err == nil {
			t.Errorf("Remove() failed, expected error, got nil")
		}
	})
}

func TestDataRangeSize(t *testing.T) {
	test.Run(t, func() {
		dataRange, err := storage.NewDataRange(storage.LocalFS(), "TEST_DATA_RANGE", 1, 4096)

		if err != nil {
			t.Errorf("NewDataRange() failed, expected nil, got %s", err)
		}

		if dataRange == nil {
			t.Errorf("NewDataRange() failed, expected not nil, got nil")
		}

		size, err := dataRange.Size()

		if err != nil {
			t.Errorf("Size() failed, expected nil, got %s", err)
		}

		if size != 0 {
			t.Errorf("Size() failed, expected 0, got %d", size)
		}

		// Write some data to the data range
		n, err := dataRange.WriteAt([]byte("test"), int64(1))

		if err != nil {
			t.Errorf("WriteAt() failed, expected nil, got %s", err)
		}

		if n != 4 {
			t.Errorf("WriteAt() failed, expected 4, got %d", n)
		}

		size, err = dataRange.Size()

		if err != nil {
			t.Errorf("Size() failed, expected nil, got %s", err)
		}

		if size != 4 {
			t.Errorf("Size() failed, expected 4096, got %d", size)
		}
	})
}

func TestDataRangeTruncate(t *testing.T) {
	test.Run(t, func() {
		dataRange, err := storage.NewDataRange(storage.LocalFS(), "TEST_DATA_RANGE", 1, 4096)

		if err != nil {
			t.Errorf("NewDataRange() failed, expected nil, got %s", err)
		}

		if dataRange == nil {
			t.Errorf("NewDataRange() failed, expected not nil, got nil")
		}

		n, err := dataRange.WriteAt([]byte("test"), int64(1))

		if err != nil {
			t.Errorf("WriteAt() failed, expected nil, got %s", err)
		}

		if n != 4 {
			t.Errorf("WriteAt() failed, expected 4, got %d", n)
		}

		err = dataRange.Truncate(1024)

		if err != nil {
			t.Errorf("Truncate() failed, expected nil, got %s", err)
		}
	})
}
