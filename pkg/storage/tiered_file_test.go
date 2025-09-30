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

func TestTieredFile(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("Create", func(t *testing.T) {
			file, err := app.Cluster.LocalFS().Create("text.txt")

			if err != nil {
				t.Error(err)
			}

			tfsd := storage.NewTieredFileSystemDriver(
				context.Background(),
				app.Cluster.NetworkFS(),
				app.Cluster.ObjectFS(),
				func(ctx context.Context, fsd *storage.TieredFileSystemDriver) {
					fsd.CanSyncDirtyFiles = func() bool {
						return true
					}
				},
			)

			defer func() {
				if err := tfsd.Shutdown(); err != nil {
					t.Errorf("error shutting down tiered file system driver: %v", err)
				}
			}()

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

			if !tf.WrittenToDurableStorageAt.IsZero() {
				t.Error("WrittenToDurableStorageAt is not zero")
			}
		})

		t.Run("Close", func(t *testing.T) {
			file, err := app.Cluster.LocalFS().Create("text.txt")

			if err != nil {
				t.Error(err)
			}

			tfsd := storage.NewTieredFileSystemDriver(
				context.Background(),
				app.Cluster.NetworkFS(),
				app.Cluster.ObjectFS(),
				func(ctx context.Context, fsd *storage.TieredFileSystemDriver) {
					fsd.CanSyncDirtyFiles = func() bool {
						return true
					}
				},
			)

			defer func() {
				if err := tfsd.Shutdown(); err != nil {
					t.Errorf("error shutting down tiered file system driver: %v", err)
				}
			}()

			tf := storage.NewTieredFile(tfsd, "test.txt", file, 0)

			if tf == nil {
				t.Error("TieredFile is nil")
			}

			if err := tf.Close(); err != nil {
				t.Error(err)
			}
		})

		t.Run("MarkUpdated", func(t *testing.T) {
			file, err := app.Cluster.LocalFS().Create("text.txt")

			if err != nil {
				t.Error(err)
			}

			tfsd := storage.NewTieredFileSystemDriver(
				context.Background(),
				app.Cluster.NetworkFS(),
				app.Cluster.ObjectFS(),
				func(ctx context.Context, fsd *storage.TieredFileSystemDriver) {
					fsd.CanSyncDirtyFiles = func() bool {
						return true
					}
				},
			)

			defer func() {
				if err := tfsd.Shutdown(); err != nil {
					t.Errorf("error shutting down tiered file system driver: %v", err)
				}
			}()

			tf := storage.NewTieredFile(tfsd, "test.txt", file, 0)

			if tf == nil {
				t.Error("TieredFile is nil")
			}

			tf.MarkUpdated()

			if tf != nil && tf.UpdatedAt.IsZero() {
				t.Error("UpdatedAt is zero")
			}
		})

		t.Run("Read", func(t *testing.T) {
			tfsd := storage.NewTieredFileSystemDriver(
				context.Background(),
				app.Cluster.NetworkFS(),
				app.Cluster.ObjectFS(),
				func(ctx context.Context, fsd *storage.TieredFileSystemDriver) {
					fsd.CanSyncDirtyFiles = func() bool {
						return true
					}
				},
			)

			defer func() {
				if err := tfsd.Shutdown(); err != nil {
					t.Errorf("error shutting down tiered file system driver: %v", err)
				}
			}()

			tf, err := tfsd.Create("test.txt")

			if err != nil {
				t.Error(err)
			}

			if tf == nil {
				t.Error("TieredFile is nil")
				return
			}

			n1, err := tf.WriteString("Hello, World!")

			if err != nil {
				t.Error(err)
			}

			if n1 != 13 {
				t.Errorf("WriteString bytes count is unexpected: %v", n1)
			}

			// Read the file
			buf := make([]byte, n1)

			s1, err := tf.Seek(0, io.SeekStart)

			if err != nil {
				t.Error(err)
			}

			if s1 != 0 {
				t.Errorf("Seek position is unexpected: %v", s1)
			}

			r1, err := tf.Read(buf)

			if err != nil {
				t.Error(err)
			}

			if r1 != n1 {
				t.Errorf("Read bytes count is unexpected: %v", r1)
			}

			if string(buf[:n1]) != "Hello, World!" {
				t.Errorf("Read content is unexpected: %v", string(buf[:n1]))
			}

			_, err = tf.Seek(0, 0)

			if err != nil {
				t.Error(err)
			}

			if tf.(*storage.TieredFileDescriptor).File.Closed {
				t.Error("Closed is false")
			}

			// Reset the buffer
			buf = make([]byte, 1024)

			// Attempt to read the file again, it should be reopened automatically
			r2, err := tf.Read(buf)

			if err != nil && err != io.EOF {
				t.Error(err)
			}

			if r2 != n1 {
				t.Errorf("Read bytes count is unexpected after reopening: %v", r2)
			}

			if string(buf[:n1]) != "Hello, World!" {
				t.Error("Read content is unexpected after reopening: " + string(buf[:n1]))
			}
		})

		t.Run("ReadAt", func(t *testing.T) {
			tfsd := storage.NewTieredFileSystemDriver(
				context.Background(),
				app.Cluster.NetworkFS(),
				app.Cluster.ObjectFS(),
				func(ctx context.Context, fsd *storage.TieredFileSystemDriver) {
					fsd.CanSyncDirtyFiles = func() bool {
						return true
					}
				},
			)

			defer func() {
				if err := tfsd.Shutdown(); err != nil {
					t.Errorf("error shutting down tiered file system driver: %v", err)
				}
			}()

			tf, err := tfsd.Create("test.txt")

			if err != nil {
				t.Error(err)
			}

			if tf == nil {
				t.Error("TieredFile is nil")
				return
			}

			data := make([]byte, 4096)

			if _, err := rand.Read(data); err != nil {
				t.Fatalf("Failed to read random data: %v", err)
			}

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

		t.Run("Seek", func(t *testing.T) {
			tfsd := storage.NewTieredFileSystemDriver(
				context.Background(),
				app.Cluster.NetworkFS(),
				app.Cluster.ObjectFS(),
				func(ctx context.Context, fsd *storage.TieredFileSystemDriver) {
					fsd.CanSyncDirtyFiles = func() bool {
						return true
					}
				},
			)

			defer func() {
				if err := tfsd.Shutdown(); err != nil {
					t.Errorf("error shutting down tiered file system driver: %v", err)
				}
			}()

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

		t.Run("Stat", func(t *testing.T) {
			tf, err := app.Cluster.LocalFS().Create("test_stat.txt")

			if err != nil {
				t.Error(err)
			}

			defer func() {
				if err := tf.Close(); err != nil {
					t.Errorf("error closing tiered file: %v", err)
				}
			}()

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

		t.Run("Sync", func(t *testing.T) {
			tfsd := storage.NewTieredFileSystemDriver(
				context.Background(),
				app.Cluster.NetworkFS(),
				app.Cluster.ObjectFS(),
				func(ctx context.Context, fsd *storage.TieredFileSystemDriver) {
					fsd.CanSyncDirtyFiles = func() bool {
						return true
					}
				},
			)

			defer func() {
				if err := tfsd.Shutdown(); err != nil {
					t.Errorf("error shutting down tiered file system driver: %v", err)
				}
			}()

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

		t.Run("Truncate", func(t *testing.T) {
			tfsd := storage.NewTieredFileSystemDriver(
				context.Background(),
				app.Cluster.NetworkFS(),
				app.Cluster.ObjectFS(),
				func(ctx context.Context, fsd *storage.TieredFileSystemDriver) {
					fsd.CanSyncDirtyFiles = func() bool {
						return true
					}
				},
			)

			defer func() {
				if err := tfsd.Shutdown(); err != nil {
					t.Errorf("error shutting down tiered file system driver: %v", err)
				}
			}()

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

		t.Run("Write", func(t *testing.T) {
			tfsd := storage.NewTieredFileSystemDriver(
				context.Background(),
				app.Cluster.NetworkFS(),
				app.Cluster.ObjectFS(),
				func(ctx context.Context, fsd *storage.TieredFileSystemDriver) {
					fsd.CanSyncDirtyFiles = func() bool {
						return true
					}
				},
			)

			defer func() {
				if err := tfsd.Shutdown(); err != nil {
					t.Errorf("error shutting down tiered file system driver: %v", err)
				}
			}()

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

			if _, err := tf.Seek(0, io.SeekStart); err != nil {
				t.Error(err)
			}

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

		t.Run("WriteAt", func(t *testing.T) {
			tfsd := storage.NewTieredFileSystemDriver(
				context.Background(),
				app.Cluster.NetworkFS(),
				app.Cluster.ObjectFS(),
				func(ctx context.Context, fsd *storage.TieredFileSystemDriver) {
					fsd.CanSyncDirtyFiles = func() bool {
						return true
					}
				},
			)

			defer func() {
				if err := tfsd.Shutdown(); err != nil {
					t.Errorf("error shutting down tiered file system driver: %v", err)
				}
			}()

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

			if _, err := tf.Seek(0, io.SeekStart); err != nil {
				t.Error(err)
			}

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

		t.Run("WriteAt_Persistence", func(t *testing.T) {
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

			defer func() {
				if err := tfsd.Shutdown(); err != nil {
					t.Errorf("error shutting down tiered file system driver: %v", err)
				}
			}()

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

		t.Run("WriteTo", func(t *testing.T) {
			tfsd := storage.NewTieredFileSystemDriver(
				context.Background(),
				app.Cluster.NetworkFS(),
				app.Cluster.ObjectFS(),
				func(ctx context.Context, fsd *storage.TieredFileSystemDriver) {
					fsd.CanSyncDirtyFiles = func() bool {
						return true
					}
				},
			)

			defer func() {
				if err := tfsd.Shutdown(); err != nil {
					t.Errorf("error shutting down tiered file system driver: %v", err)
				}
			}()

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

			if _, err := tf.Seek(0, io.SeekStart); err != nil {
				t.Error(err)
			}

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

		t.Run("WriteString", func(t *testing.T) {

		})

		t.Run("ShouldBeWrittenToDurableStorage", func(t *testing.T) {
			file, err := app.Cluster.LocalFS().Create("test_should_be_written.txt")

			if err != nil {
				t.Error(err)
			}

			tfsd := storage.NewTieredFileSystemDriver(
				context.Background(),
				app.Cluster.NetworkFS(),
				app.Cluster.ObjectFS(),
				func(ctx context.Context, fsd *storage.TieredFileSystemDriver) {
					fsd.CanSyncDirtyFiles = func() bool {
						return true
					}
				},
			)

			defer func() {
				if err := tfsd.Shutdown(); err != nil {
					t.Errorf("error shutting down tiered file system driver: %v", err)
				}
			}()

			tf := storage.NewTieredFile(tfsd, "test_should_be_written.txt", file, 0)

			t.Run("NoUpdates", func(t *testing.T) {
				if tf.ShouldBeWrittenToDurableStorage() {
					t.Error("File with no updates should not need to be written to durable storage")
				}
			})

			t.Run("UpdatedNeverWritten", func(t *testing.T) {
				now := time.Now()
				tf.SetUpdatedAt(now.Add(-2 * tfsd.WriteInterval)) // Updated 2 intervals ago
				tf.SetWrittenAt(time.Time{})                      // Never written (zero time)

				if !tf.ShouldBeWrittenToDurableStorage() {
					t.Error("File that has been updated but never written should need to be written to durable storage")
				}
			})

			t.Run("UpdatedRecently", func(t *testing.T) {
				now := time.Now()
				// File was written to durable storage recently (half interval ago)
				tf.SetWrittenAt(now.Add(-tfsd.WriteInterval / 2))
				// File was updated even more recently (quarter interval ago), after being written
				tf.SetUpdatedAt(now.Add(-tfsd.WriteInterval / 4))

				// Should NOT need writing because even though the file was updated after being written,
				// not enough time has passed since the write (< WriteInterval)
				if tf.ShouldBeWrittenToDurableStorage() {
					t.Error("File updated recently but written to durable storage within WriteInterval should not need to be written to durable storage")
				}
			})

			t.Run("UpdatedAfterWrittenWithEnoughTime", func(t *testing.T) {
				now := time.Now()
				tf.SetWrittenAt(now.Add(-2 * tfsd.WriteInterval)) // Written 2 intervals ago
				tf.SetUpdatedAt(now.Add(-tfsd.WriteInterval))     // Updated 1 interval ago (after being written)

				if !tf.ShouldBeWrittenToDurableStorage() {
					t.Error("File updated after being written with enough time passed should need to be written to durable storage")
				}
			})

			t.Run("UpdatedAfterWrittenNotEnoughTime", func(t *testing.T) {
				now := time.Now()
				tf.SetWrittenAt(now.Add(-tfsd.WriteInterval / 2)) // Written half interval ago
				tf.SetUpdatedAt(now.Add(-tfsd.WriteInterval / 4)) // Updated quarter interval ago (after being written)

				if tf.ShouldBeWrittenToDurableStorage() {
					t.Error("File updated after being written but not enough time passed should not need to be written to durable storage")
				}
			})

			t.Run("WrittenAfterUpdated", func(t *testing.T) {
				now := time.Now()
				tf.SetUpdatedAt(now.Add(-2 * tfsd.WriteInterval)) // Updated 2 intervals ago
				tf.SetWrittenAt(now.Add(-tfsd.WriteInterval))     // Written 1 interval ago (after being updated)

				if tf.ShouldBeWrittenToDurableStorage() {
					t.Error("File that was written after being updated should not need to be written to durable storage")
				}
			})

			t.Run("UpdatedAndWrittenSameTime", func(t *testing.T) {
				now := time.Now().Add(-2 * tfsd.WriteInterval)
				tf.SetUpdatedAt(now)
				tf.SetWrittenAt(now)

				if tf.ShouldBeWrittenToDurableStorage() {
					t.Error("File updated and written at the same time should not need to be written to durable storage")
				}
			})

			t.Run("RealTimingBehavior", func(t *testing.T) {
				// Reset timestamps
				tf.SetUpdatedAt(time.Time{})
				tf.SetWrittenAt(time.Time{})

				// Mark as updated
				tf.MarkUpdated()

				// Simulate that the file was updated long ago and has an old write timestamp
				oldTime := time.Now().Add(-2 * tfsd.WriteInterval)
				tf.SetUpdatedAt(oldTime.Add(tfsd.WriteInterval)) // Updated 1 interval ago
				tf.SetWrittenAt(oldTime)                         // Written 2 intervals ago

				// Now it should need writing since it was updated after being written AND enough time has passed since the update
				if !tf.ShouldBeWrittenToDurableStorage() {
					t.Error("File updated with old write timestamp should need to be written to durable storage")
				}
			})
		})
	})
}
