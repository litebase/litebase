package file

type Proxy interface {
	ReadAt(p []byte, off int64) (n int, err error)
	WriteAt(p []byte, off int64) (n int, err error)
	WritePages(pages []struct {
		Data   []byte
		Length int64
		Offset int64
	}) error
	Size() (int64, error)
}
