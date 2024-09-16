package storage_test

import (
	"context"
	"io"
	"litebase/internal/test"
	"litebase/server/storage"
	"testing"
)

func TestNewTieredFile(t *testing.T) {
	test.Run(t, func() {
		file, err := storage.LocalFS().Create("text.txt")

		if err != nil {
			t.Error(err)
		}

		tfsd := storage.NewTieredFileSystemDriver(
			context.Background(),
			storage.LocalFS(),
			storage.ObjectFS(),
		)

		tf := storage.NewTieredFile(tfsd, "test.txt", file, 0)

		if tf == nil {
			t.Error("TieredFile is nil")
		}

		if tf != nil && tf.Key != "test.txt" {
			t.Errorf("Key is unexpected: %v", tf.Key)
		}

		if tf.Flag != 0 {
			t.Errorf("Flag is unexpected: %v", tf.Flag)
		}

		if tf.File == nil {
			t.Error("File is nil")
		}

		if tf.CreatedAt.IsZero() {
			t.Error("CreatedAt is zero")
		}

		if !tf.UpdatedAt.IsZero() {
			t.Error("UpdatedAt is not zero")
		}

		if !tf.WrittenAt.IsZero() {
			t.Error("WrittenAt is not zero")
		}
	})
}

func TestTieredFileClose(t *testing.T) {
	test.Run(t, func() {
		// fs := storage.NewLocalFileSystemDriver(config.Get().DataPath + "/local")

		file, err := storage.LocalFS().Create("text.txt")

		if err != nil {
			t.Error(err)
		}

		tfsd := storage.NewTieredFileSystemDriver(
			context.Background(),
			storage.LocalFS(),
			storage.ObjectFS(),
		)

		tf := storage.NewTieredFile(tfsd, "test.txt", file, 0)

		if tf == nil {
			t.Error("TieredFile is nil")
		}

		if err := tf.Close(); err != nil {
			t.Error(err)
		}
	})
}

func TestTieredFileMarkUpdated(t *testing.T) {
	test.Run(t, func() {
		file, err := storage.LocalFS().Create("text.txt")

		if err != nil {
			t.Error(err)
		}

		tfsd := storage.NewTieredFileSystemDriver(
			context.Background(),
			storage.LocalFS(),
			storage.ObjectFS(),
		)

		tf := storage.NewTieredFile(tfsd, "test.txt", file, 0)

		if tf == nil {
			t.Error("TieredFile is nil")
		}

		tf.MarkUpdated()

		if tf != nil && tf.UpdatedAt.IsZero() {
			t.Error("UpdatedAt is zero")
		}
	})
}

func TestTieredFileRead(t *testing.T) {
	test.Run(t, func() {
		tfsd := storage.NewTieredFileSystemDriver(
			context.Background(),
			storage.LocalFS(),
			storage.ObjectFS(),
		)

		tf, err := tfsd.Create("test.txt")

		if err != nil {
			t.Error(err)
		}

		if tf == nil {
			t.Error("TieredFile is nil")
			return
		}

		_, err = tf.WriteString("Hello, World!")

		if err != nil {
			t.Error(err)
		}

		// Read the file
		buf := make([]byte, 1024)

		_, err = tf.Seek(0, 0)

		if err != nil {
			t.Error(err)
		}

		n, err := tf.Read(buf)

		if err != nil {
			t.Error(err)
		}

		if n == 0 {
			t.Error("Read failed")
		}

		if string(buf[:n]) != "Hello, World!" {
			t.Errorf("Read content is unexpected: %v", string(buf[:n]))
		}

		// Close the file
		if err := tf.Close(); err != nil {
			t.Error(err)
		}

		_, err = tf.Seek(0, 0)

		if err != nil {
			t.Error(err)
		}

		if !tf.(*storage.TieredFile).Closed {
			t.Error("Closed is not true")
		}

		// Reset the buffer
		buf = make([]byte, 1024)

		// Attempt to read the file again, it should be reopened automatically
		_, err = tf.Read(buf)

		if err != nil && err != io.EOF {
			t.Error(err)
		}

		if string(buf[:n]) == "Hello, World!" {
			t.Error("Read content should be empty since has not been synced")
		}
	})
}
