package storage_test

import (
	"bytes"
	"crypto/sha256"
	"io"
	"os"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/storage"
	"github.com/litebase/litebase/server"
)

func TestNewObjectFile(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		client := storage.NewObjectFileSystemDriver(
			app.Config,
		)
		key := "test.txt"

		of, err := storage.NewObjectFile(client, key, os.O_CREATE|os.O_RDWR, false)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if of == nil {
			t.Error("ObjectFile is nil")
		}

		if of != nil && of.Key != key {
			t.Errorf("Key is unexpected: %v", of.Key)
		}

		if of.FileInfo == (storage.StaticFileInfo{}) {
			t.Error("FileInfo is nil")
		}

		if of.OpenFlags != os.O_CREATE|os.O_RDWR {
			t.Errorf("OpenFlags is unexpected: %v", of.OpenFlags)
		}

		emptyChecksum := sha256.Sum256([]byte{})

		if !bytes.Equal(of.Sha256Checksum[:], emptyChecksum[:]) {
			t.Error("sha256Checksum is not empty")
		}
	})
}

func TestObjectFileClose(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		t.Run("no data", func(t *testing.T) {
			client := storage.NewObjectFileSystemDriver(
				app.Config,
			)
			key := "test.txt"

			of, err := storage.NewObjectFile(client, key, os.O_CREATE|os.O_RDWR, false)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			err = of.Close()

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})

		t.Run("data unchanged", func(t *testing.T) {
			client := storage.NewObjectFileSystemDriver(app.Config)
			key := "test.txt"

			of, err := storage.NewObjectFile(client, key, os.O_CREATE|os.O_RDWR, false)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			of.Data = []byte("test data")
			of.Sha256Checksum = sha256.Sum256(of.Data)

			err = of.Close()

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})

		t.Run("data changed", func(t *testing.T) {
			client := storage.NewObjectFileSystemDriver(app.Config)
			key := "test.txt"

			of, err := storage.NewObjectFile(client, key, os.O_CREATE|os.O_RDWR, false)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			of.Data = []byte("test data")

			err = of.Close()

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	})
}

func TestObjectFileRead(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		t.Run("no data", func(t *testing.T) {
			client := storage.NewObjectFileSystemDriver(app.Config)
			key := "test.txt"

			err := app.Cluster.ObjectFS().WriteFile(key, []byte{}, 0644)

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			of, err := storage.NewObjectFile(client, key, os.O_CREATE|os.O_RDWR, false)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			buf := make([]byte, 10)
			n, err := of.Read(buf)

			if err != nil && err != io.EOF {
				t.Errorf("unexpected error: %v", err)
			}

			if n != 0 {
				t.Errorf("unexpected number of bytes read: %v", n)
			}
		})

		t.Run("with data", func(t *testing.T) {
			client := storage.NewObjectFileSystemDriver(app.Config)
			key := "test.txt"

			err := app.Cluster.ObjectFS().WriteFile(key, []byte("test data"), 0644)

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			of, err := storage.NewObjectFile(client, key, os.O_CREATE|os.O_RDWR, false)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			buf := make([]byte, 10)
			n, err := of.Read(buf)

			if err != nil && err != io.EOF {
				t.Errorf("unexpected error: %v", err)
			}

			if n != len(of.Data) {
				t.Errorf("unexpected number of bytes read: %v", n)
			}

			if !bytes.Equal(buf[:n], of.Data) {
				t.Errorf("unexpected data read: %v", buf[:n])
			}
		})

		t.Run("read beyond data", func(t *testing.T) {
			client := storage.NewObjectFileSystemDriver(app.Config)
			key := "test.txt"

			of, err := storage.NewObjectFile(client, key, os.O_CREATE|os.O_RDWR, false)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			of.Data = []byte("test data")

			buf := make([]byte, 20)
			n, err := of.Read(buf)

			if err != nil && err != io.EOF {
				t.Errorf("unexpected error: %v", err)
			}

			if n != len(of.Data) {
				t.Errorf("unexpected number of bytes read: %v", n)
			}

			if !bytes.Equal(buf[:n], of.Data) {
				t.Errorf("unexpected data read: %v", buf[:n])
			}
		})
	})
}

func TestObjectFileReadAt(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		t.Run("no data", func(t *testing.T) {
			client := storage.NewObjectFileSystemDriver(
				app.Config,
			)
			key := "test.txt"

			of, err := storage.NewObjectFile(client, key, os.O_CREATE|os.O_RDWR, false)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			buf := make([]byte, 10)
			n, err := of.ReadAt(buf, 0)

			if err != nil && err != io.EOF {
				t.Errorf("unexpected error: %v", err)
			}

			if n != 0 {
				t.Errorf("unexpected number of bytes read: %v", n)
			}
		})

		t.Run("with data", func(t *testing.T) {
			client := storage.NewObjectFileSystemDriver(app.Config)
			key := "test.txt"

			of, err := storage.NewObjectFile(client, key, os.O_CREATE|os.O_RDWR, false)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			of.Data = []byte("test data")

			buf := make([]byte, 10)
			n, err := of.ReadAt(buf, 0)

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if n != len(of.Data) {
				t.Errorf("unexpected number of bytes read: %v", n)
			}

			if !bytes.Equal(buf[:n], of.Data) {
				t.Errorf("unexpected data read: %v", buf[:n])
			}
		})

		t.Run("read beyond data", func(t *testing.T) {
			client := storage.NewObjectFileSystemDriver(
				app.Config,
			)
			key := "test.txt"

			of, err := storage.NewObjectFile(client, key, os.O_CREATE|os.O_RDWR, false)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			of.Data = []byte("test data")

			buf := make([]byte, 20)
			n, err := of.ReadAt(buf, 0)

			if err != nil && err != io.EOF {
				t.Errorf("unexpected error: %v", err)
			}

			if n != len(of.Data) {
				t.Errorf("unexpected number of bytes read: %v", n)
			}

			if !bytes.Equal(buf[:n], of.Data) {
				t.Errorf("unexpected data read: %x", buf[:n])
			}
		})
	})
}

func TestObjectFileSeek(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		t.Run("no data", func(t *testing.T) {
			client := storage.NewObjectFileSystemDriver(
				app.Config,
			)
			key := "test.txt"

			of, err := storage.NewObjectFile(client, key, os.O_CREATE|os.O_RDWR, false)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			offset, err := of.Seek(0, io.SeekStart)

			if err != nil && err != io.EOF {
				t.Errorf("unexpected error: %v", err)
			}

			if offset != 0 {
				t.Errorf("unexpected offset: %v", offset)
			}
		})

		t.Run("with data", func(t *testing.T) {
			client := storage.NewObjectFileSystemDriver(app.Config)
			key := "test.txt"

			of, err := storage.NewObjectFile(client, key, os.O_CREATE|os.O_RDWR, false)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			of.Data = []byte("test data")

			offset, err := of.Seek(0, io.SeekStart)

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if offset != 0 {
				t.Errorf("unexpected offset: %v", offset)
			}

			offset, err = of.Seek(4, io.SeekStart)

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if offset != 4 {
				t.Errorf("unexpected offset: %v", offset)
			}

			offset, err = of.Seek(0, io.SeekEnd)

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if offset != int64(len(of.Data)) {
				t.Errorf("unexpected offset: %v", offset)
			}
		})
	})
}

func TestObjectFileStat(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		client := storage.NewObjectFileSystemDriver(app.Config)
		key := "test.txt"

		of, err := storage.NewObjectFile(client, key, os.O_CREATE|os.O_RDWR, false)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		fi, err := of.Stat()

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if fi == nil {
			t.Error("FileInfo is nil")
		}
	})
}

func TestObjectFileSync(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		client := storage.NewObjectFileSystemDriver(
			app.Config,
		)
		key := "test.txt"
		openFlags := os.O_RDONLY

		of, err := storage.NewObjectFile(client, key, openFlags, false)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		err = of.Sync()

		if err != os.ErrPermission {
			t.Errorf("unexpected error: %v", err)
		}

		of.OpenFlags = os.O_RDWR

		err = of.Sync()

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func TestObjectFileTruncate(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		client := storage.NewObjectFileSystemDriver(
			app.Config,
		)
		key := "test.txt"

		of, err := storage.NewObjectFile(client, key, os.O_CREATE|os.O_RDWR, false)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		of.Data = []byte("Hello World! Here we are...")

		err = of.Truncate(10)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if len(of.Data) != 10 {
			t.Errorf("unexpected data length: %v", len(of.Data))
		}
	})
}

func TestObjectFileWithData(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		client := storage.NewObjectFileSystemDriver(
			app.Config,
		)
		key := "test.txt"

		of, err := storage.NewObjectFile(client, key, os.O_CREATE|os.O_RDWR, false)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		of.WithData([]byte("test data"))

		if !bytes.Equal(of.Data, []byte("test data")) {
			t.Errorf("unexpected data: %v", of.Data)
		}
	})
}

func TestObjectFileWrite(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		client := storage.NewObjectFileSystemDriver(app.Config)
		key := "test.txt"

		of, err := storage.NewObjectFile(client, key, os.O_CREATE|os.O_RDWR, false)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		n, err := of.Write([]byte("Hello World!"))

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if n != len("Hello World!") {
			t.Errorf("unexpected number of bytes written: %v", n)
		}

		if string(of.Data) != "Hello World!" {
			t.Errorf("unexpected data: %v", string(of.Data))
		}

		n, err = of.Write([]byte("Hello World!"))

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if n != len("Hello World!") {
			t.Errorf("unexpected number of bytes written: %v", n)
		}

		if string(of.Data) != "Hello World!Hello World!" {
			t.Errorf("unexpected data: %v", string(of.Data))
		}
	})
}

func TestObjectFileWriteAt(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		client := storage.NewObjectFileSystemDriver(app.Config)
		key := "test.txt"

		of, err := storage.NewObjectFile(client, key, os.O_CREATE|os.O_RDWR, false)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		of.Data = []byte("Hello World!")

		n, err := of.WriteAt([]byte("Friend!"), 6)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if n != len("Friend!") {
			t.Errorf("unexpected number of bytes written: %v", n)
		}

		if string(of.Data) != "Hello Friend!" {
			t.Errorf("unexpected data: %v", string(of.Data))
		}
	})
}

func TestObjectFileWriteTo(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		client := storage.NewObjectFileSystemDriver(app.Config)
		key := "test.txt"

		of, err := storage.NewObjectFile(client, key, os.O_CREATE|os.O_RDWR, false)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		of.Data = []byte("Hello World!")

		buf := new(bytes.Buffer)
		n, err := of.WriteTo(buf)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if n != int64(len(of.Data)) {
			t.Errorf("unexpected number of bytes written: %v", n)
		}

		if buf.String() != string(of.Data) {
			t.Errorf("unexpected data: %v", buf.String())
		}
	})
}

func TestObjectFileWriteString(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		client := storage.NewObjectFileSystemDriver(app.Config)
		key := "test.txt"

		of, err := storage.NewObjectFile(client, key, os.O_CREATE|os.O_RDWR, false)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		n, err := of.WriteString("Hello World!")

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if n != len("Hello World!") {
			t.Errorf("unexpected number of bytes written: %v", n)
		}

		if string(of.Data) != "Hello World!" {
			t.Errorf("unexpected data: %v", string(of.Data))
		}
	})
}
