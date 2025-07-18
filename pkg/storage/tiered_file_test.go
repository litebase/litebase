package storage_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/storage"
)

func TestNewTieredFile(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		file, err := app.Cluster.LocalFS().Create("text.txt")

		if err != nil {
			t.Error(err)
		}

		tfsd := storage.NewTieredFileSystemDriver(
			context.Background(),
			app.Cluster.LocalFS(),
			app.Cluster.ObjectFS(),
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

func TestTieredFile_Close(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		file, err := app.Cluster.LocalFS().Create("text.txt")

		if err != nil {
			t.Error(err)
		}

		tfsd := storage.NewTieredFileSystemDriver(
			context.Background(),
			app.Cluster.LocalFS(),
			app.Cluster.ObjectFS(),
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

func TestTieredFile_MarkUpdated(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		file, err := app.Cluster.LocalFS().Create("text.txt")

		if err != nil {
			t.Error(err)
		}

		tfsd := storage.NewTieredFileSystemDriver(
			context.Background(),
			app.Cluster.LocalFS(),
			app.Cluster.ObjectFS(),
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

func TestTieredFile_Read(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		tfsd := storage.NewTieredFileSystemDriver(
			context.Background(),
			app.Cluster.LocalFS(),
			app.Cluster.ObjectFS(),
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

		_, err = tf.Seek(0, 0)

		if err != nil {
			t.Error(err)
		}

		if tf.(*storage.TieredFile).Closed {
			t.Error("Closed is false")
		}

		// Reset the buffer
		buf = make([]byte, 1024)

		// Attempt to read the file again, it should be reopened automatically
		_, err = tf.Read(buf)

		if err != nil && err != io.EOF {
			t.Error(err)
		}

		if string(buf[:n]) != "Hello, World!" {
			t.Error("Read content is unexpected after reopening: " + string(buf[:n]))
		}
	})
}

func TestTieredFileReadAt(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		tfsd := storage.NewTieredFileSystemDriver(
			context.Background(),
			app.Cluster.LocalFS(),
			app.Cluster.ObjectFS(),
		)

		tf, err := tfsd.Create("test.txt")

		if err != nil {
			t.Error(err)
		}

		if tf == nil {
			t.Error("TieredFile is nil")
			return
		}

		data := make([]byte, 4096)

		rand.Read(data)

		_, err = tf.WriteAt(data, 0)

		if err != nil {
			t.Error(err)
		}

		buf := make([]byte, 4096)

		n, err := tf.ReadAt(buf, 0)

		if err != nil {
			t.Error(err)
		}

		if n == 0 {
			t.Error("ReadAt failed")
		}

		if !bytes.Equal(data[:n], buf[:n]) {
			t.Errorf("ReadAt content is unexpected: %v", string(buf[:n]))
		}

		if err := tf.Close(); err != nil {
			t.Error(err)
		}
	})
}

func TestTieredFile_Seek(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		tfsd := storage.NewTieredFileSystemDriver(
			context.Background(),
			app.Cluster.LocalFS(),
			app.Cluster.ObjectFS(),
		)

		tf, err := tfsd.Create("test.txt")

		if err != nil {
			t.Error(err)
		}

		if tf == nil {
			t.Fatal("TieredFile is nil")
		}

		data1 := []byte("Hello")
		_, err = tf.Write(data1)
		if err != nil {
			t.Error(err)
		}

		data2 := []byte("World")
		_, err = tf.Write(data2)
		if err != nil {
			t.Error(err)
		}

		offset, err := tf.Seek(0, io.SeekStart)

		if err != nil {
			t.Error(err)
		}

		if offset != 0 {
			t.Errorf("Seek offset is unexpected: %v", offset)
		}

		buf := make([]byte, 5)

		n, err := tf.Read(buf)

		if err != nil {
			t.Error(err)
		}

		if n != 5 {
			t.Errorf("Read bytes count is unexpected: %v", n)
		}

		if string(buf[:n]) != "Hello" {
			t.Errorf("Read content is unexpected: %v", string(buf[:n]))
		}

		offset, err = tf.Seek(5, io.SeekStart)

		if err != nil {
			t.Error(err)
		}

		if offset != 5 {
			t.Errorf("Seek offset is unexpected: %v", offset)
		}

		buf = make([]byte, 5)

		n, err = tf.Read(buf)

		if err != nil {
			t.Error(err)
		}

		if string(buf[:n]) != "World" {
			t.Errorf("Read content is unexpected: %v", string(buf[:n]))
		}

		if err := tf.Close(); err != nil {
			t.Error(err)
		}
	})
}

func TestTieredFile_Stat(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		tf, err := app.Cluster.LocalFS().Create("test_stat.txt")
		if err != nil {
			t.Error(err)
		}
		defer tf.Close()

		data := []byte("Test data for stat")
		_, err = tf.Write(data)
		if err != nil {
			t.Error(err)
		}

		info, err := tf.Stat()
		if err != nil {
			t.Error(err)
		}

		if info.Size() != int64(len(data)) {
			t.Errorf("Stat size is unexpected: %v", info.Size())
		}
	})
}

func TestTieredFile_Sync(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		tfsd := storage.NewTieredFileSystemDriver(
			context.Background(),
			app.Cluster.LocalFS(),
			app.Cluster.ObjectFS(),
		)

		tf, err := tfsd.Create("test_sync.txt")

		if err != nil {
			t.Error(err)
		}

		if tf == nil {
			t.Error("TieredFile is nil")
			return
		}

		data := []byte("Hello, Sync!")
		_, err = tf.Write(data)

		if err != nil {
			t.Error(err)
		}

		if err := tf.Sync(); err != nil {
			t.Error(err)
		}

		info, err := tf.Stat()

		if err != nil {
			t.Error(err)
		}

		if info.Size() != int64(len(data)) {
			t.Errorf("Sync size is unexpected: %v", info.Size())
		}

		if err := tf.Close(); err != nil {
			t.Error(err)
		}
	})
}

func TestTieredFile_Truncate(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		tfsd := storage.NewTieredFileSystemDriver(
			context.Background(),
			app.Cluster.LocalFS(),
			app.Cluster.ObjectFS(),
		)

		tf, err := tfsd.Create("test_truncate.txt")

		if err != nil {
			t.Error(err)
		}

		if tf == nil {
			t.Error("TieredFile is nil")
			return
		}

		data := []byte("Hello, Truncate!")
		_, err = tf.Write(data)

		if err != nil {
			t.Error(err)
		}

		if err := tf.Truncate(5); err != nil {
			t.Error(err)
		}

		info, err := tf.Stat()

		if err != nil {
			t.Error(err)
		}

		if info.Size() != 5 {
			t.Errorf("Truncate size is unexpected: %v", info.Size())
		}

		if err := tf.Close(); err != nil {
			t.Error(err)
		}
	})
}

func TestTieredFileWrite(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		tfsd := storage.NewTieredFileSystemDriver(
			context.Background(),
			app.Cluster.LocalFS(),
			app.Cluster.ObjectFS(),
		)

		tf, err := tfsd.Create("test_write.txt")

		if err != nil {
			t.Error(err)
		}

		if tf == nil {
			t.Error("TieredFile is nil")
			return
		}

		data := []byte("Hello, Write!")
		n, err := tf.Write(data)

		if err != nil {
			t.Error(err)
		}

		if n != len(data) {
			t.Errorf("Write bytes count is unexpected: %v", n)
		}

		buf := make([]byte, len(data))

		tf.Seek(0, io.SeekStart)

		n, err = tf.Read(buf)

		if err != nil {
			t.Error(err)
		}

		if string(buf[:n]) != "Hello, Write!" {
			t.Errorf("Write content is unexpected: %v", string(buf[:n]))
		}

		if err := tf.Close(); err != nil {
			t.Error(err)
		}
	})
}

func TestTieredFile_WriteAt(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		tfsd := storage.NewTieredFileSystemDriver(
			context.Background(),
			app.Cluster.LocalFS(),
			app.Cluster.ObjectFS(),
		)

		tf, err := tfsd.Create("test_writeat.txt")

		if err != nil {
			t.Error(err)
		}

		if tf == nil {
			t.Error("TieredFile is nil")
			return
		}

		data := []byte("Hello, WriteAt!")
		n, err := tf.WriteAt(data, 5)

		if err != nil {
			t.Error(err)
		}

		if n != len(data) {
			t.Errorf("WriteAt bytes count is unexpected: %v", n)
		}

		buf := make([]byte, 20)
		tf.Seek(0, io.SeekStart)

		_, err = tf.Read(buf)

		if err != nil {
			t.Error(err)
		}

		if string(buf[5:5+len(data)]) != "Hello, WriteAt!" {
			t.Errorf("WriteAt content is unexpected: %v", string(buf[5:5+len(data)]))
		}

		if err := tf.Close(); err != nil {
			t.Error(err)
		}
	})
}

func TestTieredFile_WriteAt_Persistence(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		tfsd := storage.NewTieredFileSystemDriver(
			app.Cluster.Node().Context(),
			app.Cluster.NetworkFS(),
			app.Cluster.ObjectFS(),
			func(c context.Context, fsd *storage.TieredFileSystemDriver) {
				fsd.WriteInterval = 1 * time.Millisecond
			},
			func(ctx context.Context, fsd *storage.TieredFileSystemDriver) {
				fsd.CanSyncDirtyFiles = func() bool {
					return true
				}
			},
		)

		tf, err := tfsd.Create("test_writeat.txt")

		if err != nil {
			t.Error(err)
		}

		if tf == nil {
			t.Error("TieredFile is nil")
			return
		}

		data1 := []byte("Hello,")
		n, err := tf.WriteAt(data1, 0)

		if err != nil {
			t.Error(err)
		}

		if n != len(data1) {
			t.Errorf("WriteAt bytes count is unexpected: %v", n)
		}

		data2 := []byte(" WriteAt!")

		n, err = tf.WriteAt(data2, 6)

		if err != nil {
			t.Error(err)
		}

		if n != len(data2) {
			t.Errorf("WriteAt bytes count is unexpected: %v", n)
		}

		buf := data1
		buf = append(buf, data2...) // buf should now contain "Hello, WriteAt

		time.Sleep(10 * time.Millisecond) // Wait for the background writer to flush the file

		// Get the file from object storage
		objectData, err := app.Cluster.ObjectFS().ReadFile("test_writeat.txt")

		if err != nil {
			t.Error(err)
		}

		if !bytes.Equal(buf, objectData) {
			t.Errorf("Data in object storage is unexpected: %v", string(buf))
		}
	})
}

func TestTieredFile_WriteTo(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		tfsd := storage.NewTieredFileSystemDriver(
			context.Background(),
			app.Cluster.LocalFS(),
			app.Cluster.ObjectFS(),
		)

		tf, err := tfsd.Create("test_writeto.txt")

		if err != nil {
			t.Error(err)
		}

		if tf == nil {
			t.Error("TieredFile is nil")
			return
		}

		buf := bytes.NewBuffer([]byte{})

		data := []byte("Hello, WriteTo!")

		_, err = tf.Write(data)

		if err != nil {
			t.Error(err)
		}

		tf.Seek(0, io.SeekStart)

		n, err := tf.WriteTo(buf)

		if err != nil {
			t.Error(err)
		}

		if n != int64(len(data)) {
			t.Errorf("WriteTo bytes count is unexpected: %v", n)
		}

		if buf.String() != "Hello, WriteTo!" {
			t.Errorf("WriteTo content is unexpected: %v", buf.String())
		}

		if err := tf.Close(); err != nil {
			t.Error(err)
		}
	})
}

func TestTieredFile_WriteString(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {

	})
}
