package storage_test

import (
	"testing"
	"time"

	"github.com/litebase/litebase/server/storage"
)

func TestNewStaticFileInfo(t *testing.T) {
	name := "file.txt"
	size := int64(12345)
	modTime := time.Now()

	fi := storage.NewStaticFileInfo(name, size, modTime)

	if fi == (storage.StaticFileInfo{}) {
		t.Error("NewStaticFileInfo() returned nil")
	}
}

func TestStaticFileInfoIsDir(t *testing.T) {
	tests := []struct {
		name     string
		expected bool
	}{
		{"folder/", true},
		{"file.txt", false},
	}

	for _, tt := range tests {
		fi := storage.NewStaticFileInfo(tt.name, 0, time.Now())

		if got := fi.IsDir(); got != tt.expected {
			t.Errorf("IsDir() = %v, want %v", got, tt.expected)
		}
	}
}

func TestStaticFileInfoName(t *testing.T) {
	name := "file.txt"

	fi := storage.NewStaticFileInfo(name, 0, time.Now())

	if got := fi.Name(); got != name {
		t.Errorf("Name() = %v, want %v", got, name)
	}
}

func TestStaticFileInfoSize(t *testing.T) {
	size := int64(12345)

	fi := storage.NewStaticFileInfo("file.txt", size, time.Now())

	if got := fi.Size(); got != size {
		t.Errorf("Size() = %v, want %v", got, size)
	}
}

func TestStaticFileInfoMode(t *testing.T) {
	fi := storage.StaticFileInfo{}

	if got := fi.Mode(); got != 0 {
		t.Errorf("Mode() = %v, want %v", got, 0)
	}
}

func TestStaticFileInfoModTime(t *testing.T) {
	modTime := time.Now()

	fi := storage.NewStaticFileInfo("file.txt", 0, modTime)

	if got := fi.ModTime(); !got.Equal(modTime) {
		t.Errorf("ModTime() = %v, want %v", got, modTime)
	}
}

func TestStaticFileInfoSys(t *testing.T) {
	fi := storage.StaticFileInfo{}

	if got := fi.Sys(); got != nil {
		t.Errorf("Sys() = %v, want %v", got, nil)
	}
}
