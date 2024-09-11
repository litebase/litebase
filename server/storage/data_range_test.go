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

		// Ensure the data range file is created
		_, err = storage.LocalFS().Stat(fmt.Sprintf("TEST_DATA_RANGE/%010d", 1))

		if err != nil {
			t.Errorf("NewDataRange() failed, expected nil, got %s", err)
		}
	})
}

func TestDataRangeWriteAtAndReadAt(t *testing.T) {
	test.Run(t, func() {
		dataRange, err := storage.NewDataRange(storage.LocalFS(), "TEST_DATA_RANGE", 1, 4096)

		if err != nil {
			t.Errorf("NewDataRange() failed, expected nil, got %s", err)
		}

		if dataRange == nil {
			t.Errorf("NewDataRange() failed, expected not nil, got nil")
		}

		pageNumbers := []int64{1, 2, 3, 4, 5}

		// Write some data to the data range
		for _, pageNumber := range pageNumbers {
			data := make([]byte, 4096)

			for i := 0; i < len(data); i++ {
				data[i] = byte(pageNumber)

				n, err := dataRange.WriteAt(data, pageNumber)

				if err != nil {
					t.Errorf("WriteAt() failed, expected nil, got %s", err)
				}

				if n != 4096 {
					t.Errorf("WriteAt() failed, expected 4096, got %d", n)
				}
			}
		}

		for _, pageNumber := range pageNumbers {
			data := make([]byte, 4096)

			n, err := dataRange.ReadAt(data, pageNumber)

			if err != nil {
				t.Errorf("ReadAt() failed, expected nil, got %s", err)
			}

			if n != 4096 {
				t.Errorf("ReadAt() failed, expected 4096, got %d", n)
			}
		}
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

		// Verify the data range is closed by trying to read from it
		_, err = dataRange.ReadAt([]byte{}, int64(1))

		if err == nil {
			t.Errorf("Close() failed, expected error, got nil")
		}

		// Verify the data range is closed by trying to get the size
		_, err = dataRange.Size()

		if err == nil {
			t.Errorf("Close() failed, expected error, got nil")
		}

		// Verify the data range is closed by trying to truncate it
		err = dataRange.Truncate(1024)

		if err == nil {
			t.Errorf("Close() failed, expected error, got nil")
		}

		// Verify the data range is closed by trying to write to it
		_, err = dataRange.WriteAt([]byte{}, int64(1))

		if err == nil {
			t.Errorf("Close() failed, expected error, got nil")
		}
	})
}

func TestDataRangePageCount(t *testing.T) {
	test.Run(t, func() {
		dataRange, err := storage.NewDataRange(storage.LocalFS(), "TEST_DATA_RANGE", 1, 4096)

		if err != nil {
			t.Errorf("NewDataRange() failed, expected nil, got %s", err)
		}

		if dataRange == nil {
			t.Errorf("NewDataRange() failed, expected not nil, got nil")
		}

		pageCount := dataRange.PageCount()

		if pageCount != 0 {
			t.Errorf("PageCount() failed, expected 0, got %d", pageCount)
		}

		// Write some data to the data range
		n, err := dataRange.WriteAt(make([]byte, 4096), int64(1))

		if err != nil {
			t.Errorf("WriteAt() failed, expected nil, got %s", err)
		}

		if n != 4096 {
			t.Errorf("WriteAt() failed, expected 4096, got %d", n)
		}

		pageCount = dataRange.PageCount()

		if pageCount != 1 {
			t.Errorf("PageCount() failed, expected 1, got %d", pageCount)
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
