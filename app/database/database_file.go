package database

import (
	"bytes"
	"encoding/binary"
	"log"
	"os"
	"sync"
)

type DatabaseFile struct {
	BinaryHeader []byte
	file         *os.File
	header       *DatabaseHeader
	Path         string
}

func (d *DatabaseFile) AllPages() []int {
	totalPages := int(d.header.TotalPages)
	pages := make([]int, totalPages)

	for i := 0; i < totalPages; i++ {
		pages[i] = i
	}

	return pages
}

func CreateDatabaseFile(path string) (*DatabaseFile, error) {
	databaseFile := &DatabaseFile{
		Path: path,
	}

	file, err := os.Create(path)

	if err != nil {
		return nil, err
	}

	databaseFile.file = file

	return databaseFile, nil
}

func NewDatabaseFile(path string) (*DatabaseFile, error) {
	databaseFile := &DatabaseFile{
		Path: path,
	}

	file, err := databaseFile.Open()

	if err != nil {
		return nil, err
	}

	databaseFile.file = file

	databaseFile.ReadHeader()

	return databaseFile, nil
}

func (d *DatabaseFile) Close() {
	if d.file != nil {
		d.file.Close()
	}
}

func (d *DatabaseFile) Header() *DatabaseHeader {
	return d.header
}

func (d *DatabaseFile) Open() (*os.File, error) {
	return os.OpenFile(d.Path, os.O_RDWR, 0644)
}

func (d *DatabaseFile) ReadHeader() {
	d.readHeader(d.file)
}

func (d *DatabaseFile) readHeader(file *os.File) {
	d.BinaryHeader = make([]byte, 100)
	binary.Read(file, binary.BigEndian, &d.BinaryHeader)

	if err := binary.Read(bytes.NewBuffer(d.BinaryHeader), binary.BigEndian, &d.header); err != nil {
		log.Fatal(err)
	}
}

func (d *DatabaseFile) ReadPage(pageNumber int) DatabasePage {
	data := make([]byte, d.header.PageSize)
	d.file.ReadAt(data, int64(int(d.header.PageSize)*pageNumber))

	return DatabasePage{
		Data: data,
	}
}

func (d *DatabaseFile) ReadPages(changedPages []int) map[int]DatabasePage {
	pages := make(map[int]DatabasePage, len(changedPages))

	var mutex = &sync.Mutex{}
	var wg sync.WaitGroup

	for _, pageNumber := range changedPages {
		wg.Add(1)

		go func(pageNumber int) {
			defer wg.Done()
			var page = d.ReadPage(pageNumber)

			mutex.Lock()
			pages[pageNumber] = page
			mutex.Unlock()
		}(pageNumber)
	}

	wg.Wait()

	return pages
}

func (d *DatabaseFile) WriteHeader(header []byte) {
	_, err := d.file.WriteAt(header, 0)
	d.readHeader(d.file)

	if err != nil {
		log.Fatal(err)
	}
}

func (d *DatabaseFile) WritePage(pageNumber int, page DatabasePage) {
	_, err := d.file.WriteAt(page.Data, int64(int(d.header.PageSize)*pageNumber))

	if err != nil {
		log.Fatal(err)
	}
}
