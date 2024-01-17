package file

import (
	"sync"
)

type LambdaFileProxy struct {
	databaseName string
	lambdaClient *LambdaClient
	mutex        *sync.Mutex
}

func NewLambdaFileProxy(databaseName string) *LambdaFileProxy {
	fp := &LambdaFileProxy{
		lambdaClient: NewLambdaClient(databaseName),
		mutex:        &sync.Mutex{},
		databaseName: databaseName,
	}

	return fp
}

func (fp *LambdaFileProxy) ReadAt(data []byte, offset int64) (n int, err error) {
	return fp.lambdaClient.ReadAt(data, offset)
}

func (fp *LambdaFileProxy) WriteAt(data []byte, offset int64) (n int, err error) {
	fp.mutex.Lock()
	defer fp.mutex.Unlock()

	response, err := fp.lambdaClient.Write([]struct {
		Data   []byte
		Length int64
		Offset int64
	}{
		{
			Data:   data,
			Length: int64(len(data)),
			Offset: offset,
		},
	})

	if err != nil {
		return 0, err
	}

	return int(response.Pages[0].Length), nil
}

func (fp *LambdaFileProxy) WritePages(pages []struct {
	Data   []byte
	Length int64
	Offset int64
}) error {
	_, err := fp.lambdaClient.Write(pages)

	return err
}

func (fp *LambdaFileProxy) Size() (int64, error) {
	return fp.lambdaClient.Size()
}
