package storage_test

import (
	"litebase/server/storage"
	"testing"
	"time"
)

func TestNewObjectFileInfo(t *testing.T) {
	name := "file.txt"
	size := int64(12345)
	modTime := time.Now()

	fi := storage.NewObjectFileInfo(name, size, modTime)

	if fi == nil {
		t.Error("NewObjectFileInfo() returned nil")
	}
}

func TestObjectFileInfoIsDir(t *testing.T) {
	tests := []struct {
		name     string
		expected bool
	}{
		{"folder/", true},
		{"file.txt", false},
	}

	for _, tt := range tests {
		fi := storage.NewObjectFileInfo(tt.name, 0, time.Now())

		if got := fi.IsDir(); got != tt.expected {
			t.Errorf("IsDir() = %v, want %v", got, tt.expected)
		}
	}
}

func TestObjectFileInfoName(t *testing.T) {
	name := "file.txt"

	fi := storage.NewObjectFileInfo(name, 0, time.Now())

	if got := fi.Name(); got != name {
		t.Errorf("Name() = %v, want %v", got, name)
	}
}

func TestObjectFileInfoSize(t *testing.T) {
	size := int64(12345)

	fi := storage.NewObjectFileInfo("file.txt", size, time.Now())

	if got := fi.Size(); got != size {
		t.Errorf("Size() = %v, want %v", got, size)
	}
}

func TestObjectFileInfoMode(t *testing.T) {
	fi := storage.ObjectFileInfo{}

	if got := fi.Mode(); got != 0 {
		t.Errorf("Mode() = %v, want %v", got, 0)
	}
}

func TestObjectFileInfoModTime(t *testing.T) {
	modTime := time.Now()

	fi := storage.NewObjectFileInfo("file.txt", 0, modTime)

	if got := fi.ModTime(); !got.Equal(modTime) {
		t.Errorf("ModTime() = %v, want %v", got, modTime)
	}
}

func TestObjectFileInfoSys(t *testing.T) {
	fi := storage.ObjectFileInfo{}

	if got := fi.Sys(); got != nil {
		t.Errorf("Sys() = %v, want %v", got, nil)
	}
}
