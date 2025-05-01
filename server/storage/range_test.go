package storage_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"

	"github.com/litebase/litebase/server/storage"

	"github.com/litebase/litebase/server"
)

func TestNewRange(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		r, err := storage.NewRange("databaseId", "branchId", app.Cluster.LocalFS(), "TEST_DATA_RANGE/", 1, 4096)

		if err != nil {
			t.Errorf("NewRange() failed, expected nil, got %s", err)
		}

		if r == nil {
			t.Errorf("NewRange() failed, expected not nil, got nil")
		}

		// Ensure the range file is created
		_, err = app.Cluster.LocalFS().Stat(fmt.Sprintf("TEST_DATA_RANGE/%010d", 1))

		if err != nil {
			t.Errorf("NewRange() failed, expected nil, got %s", err)
		}
	})
}

func TestRangeWriteAtAndReadAt(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		r, err := storage.NewRange("databaseId", "branchId", app.Cluster.LocalFS(), "TEST_DATA_RANGE", 1, 4096)

		if err != nil {
			t.Errorf("NewRange() failed, expected nil, got %s", err)
		}

		if r == nil {
			t.Errorf("NewRange() failed, expected not nil, got nil")
		}

		pageNumbers := []int64{1, 2, 3, 4, 5}

		// Write some data to the range
		for _, pageNumber := range pageNumbers {
			data := make([]byte, 4096)

			for i := 0; i < len(data); i++ {
				data[i] = byte(pageNumber)

				n, err := r.WriteAt(pageNumber, data)

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

			n, err := r.ReadAt(pageNumber, data)

			if err != nil {
				t.Errorf("ReadAt() failed, expected nil, got %s", err)
			}

			if n != 4096 {
				t.Errorf("ReadAt() failed, expected 4096, got %d", n)
			}
		}
	})
}

func TestRangeClose(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		r, err := storage.NewRange("databaseId", "branchId", app.Cluster.LocalFS(), "TEST_DATA_RANGE", 1, 4096)

		if err != nil {
			t.Errorf("NewRange() failed, expected nil, got %s", err)
		}

		if r == nil {
			t.Errorf("NewRange() failed, expected not nil, got nil")
		}

		err = r.Close()

		if err != nil {
			t.Errorf("Close() failed, expected nil, got %s", err)
		}

		// Verify the range is closed by trying to read from it
		_, err = r.ReadAt(int64(1), []byte{})

		if err == nil {
			t.Errorf("Close() failed, expected error, got nil")
		}

		// Verify the range is closed by trying to get the size
		_, err = r.Size()

		if err == nil {
			t.Errorf("Close() failed, expected error, got nil")
		}

		// Verify the range is closed by trying to truncate it
		err = r.Truncate(1024)

		if err == nil {
			t.Errorf("Close() failed, expected error, got nil")
		}

		// Verify the range is closed by trying to write to it
		_, err = r.WriteAt(int64(1), []byte{})

		if err == nil {
			t.Errorf("Close() failed, expected error, got nil")
		}
	})
}

func TestRangePageCount(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		r, err := storage.NewRange("databaseId", "branchId", app.Cluster.LocalFS(), "TEST_DATA_RANGE", 1, 4096)

		if err != nil {
			t.Errorf("NewRange() failed, expected nil, got %s", err)
		}

		if r == nil {
			t.Errorf("NewRange() failed, expected not nil, got nil")
		}

		pageCount := r.PageCount()

		if pageCount != 0 {
			t.Errorf("PageCount() failed, expected 0, got %d", pageCount)
		}

		data := make([]byte, 4096)

		// Write some data to the range
		n, err := r.WriteAt(int64(1), data)

		if err != nil {
			t.Errorf("WriteAt() failed, expected nil, got %s", err)
		}

		if n != 4096 {
			t.Errorf("WriteAt() failed, expected 4096, got %d", n)
		}

		pageCount = r.PageCount()

		if pageCount != 1 {
			t.Errorf("PageCount() failed, expected 1, got %d", pageCount)
		}
	})
}

func TestRangeRemove(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		r, err := storage.NewRange("databaseId", "branchId", app.Cluster.LocalFS(), "TEST_DATA_RANGE", 1, 4096)

		if err != nil {
			t.Errorf("NewRange() failed, expected nil, got %s", err)
		}

		if r == nil {
			t.Errorf("NewRange() failed, expected not nil, got nil")
		}

		err = r.Delete()

		if err != nil {
			t.Errorf("Remove() failed, expected nil, got %s", err)
		}

		// Verify the range is removed
		_, err = app.Cluster.LocalFS().Stat(fmt.Sprintf("%s/TEST_DATA_RANGE/%010d/", app.Config.DataPath, 1))

		if err == nil {
			t.Errorf("Remove() failed, expected error, got nil")
		}
	})
}

func TestRangeSize(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		r, err := storage.NewRange("databaseId", "branchId", app.Cluster.LocalFS(), "TEST_DATA_RANGE", 1, 4096)

		if err != nil {
			t.Errorf("NewRange() failed, expected nil, got %s", err)
		}

		if r == nil {
			t.Errorf("NewRange() failed, expected not nil, got nil")
		}

		size, err := r.Size()

		if err != nil {
			t.Errorf("Size() failed, expected nil, got %s", err)
		}

		if size != 0 {
			t.Errorf("Size() failed, expected 0, got %d", size)
		}

		data := make([]byte, 4096)

		// Write some data to the range
		n, err := r.WriteAt(int64(1), data)

		if err != nil {
			t.Errorf("WriteAt() failed, expected nil, got %s", err)
		}

		if n != 4096 {
			t.Errorf("WriteAt() failed, expected 4096, got %d", n)
		}

		size, err = r.Size()

		if err != nil {
			t.Errorf("Size() failed, expected nil, got %s", err)
		}

		if size != (4096) {
			t.Errorf("Size() failed, expected %d, got %d", 4096, size)
		}
	})
}

func TestRangeTruncate(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		r, err := storage.NewRange("databaseId", "branchId", app.Cluster.LocalFS(), "TEST_DATA_RANGE", 1, 4096)

		if err != nil {
			t.Errorf("NewRange() failed, expected nil, got %s", err)
		}

		if r == nil {
			t.Errorf("NewRange() failed, expected not nil, got nil")
		}

		n, err := r.WriteAt(int64(1), []byte("test"))

		if err != nil {
			t.Errorf("WriteAt() failed, expected nil, got %s", err)
		}

		if n != 4 {
			t.Errorf("WriteAt() failed, expected 4, got %d", n)
		}

		err = r.Truncate(1024)

		if err != nil {
			t.Errorf("Truncate() failed, expected nil, got %s", err)
		}
	})
}
