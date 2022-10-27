package file

import (
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

func (d *DatabaseFile) File() *os.File {
	return d.file
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
	err := binary.Read(file, binary.BigEndian, &d.BinaryHeader)

	if err != nil {
		panic(err)
	}

	d.header = &DatabaseHeader{
		HeaderString:         d.BinaryHeader[0:16],
		PageSize:             binary.BigEndian.Uint16(d.BinaryHeader[16:18]),
		WriteVersion:         d.BinaryHeader[18],
		ReadVersion:          d.BinaryHeader[19],
		ReservedSpace:        d.BinaryHeader[20],
		MaxFraction:          d.BinaryHeader[21],
		MinFraction:          d.BinaryHeader[22],
		LeafFraction:         d.BinaryHeader[23],
		ChangeCounter:        binary.BigEndian.Uint32(d.BinaryHeader[24:28]),
		TotalPages:           binary.BigEndian.Uint32(d.BinaryHeader[28:32]),
		SchemaCookie:         binary.BigEndian.Uint32(d.BinaryHeader[40:44]),
		SchemaFormat:         binary.BigEndian.Uint32(d.BinaryHeader[44:48]),
		TextEncoding:         binary.BigEndian.Uint32(d.BinaryHeader[56:60]),
		ReservedForExpansion: d.BinaryHeader[60:80],
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

func (d *DatabaseFile) WritePage(pageNumber int, page *DatabasePage) {
	_, err := d.file.WriteAt(page.Data, int64(int(d.header.PageSize)*pageNumber))

	if err != nil {
		log.Fatal(err)
	}
}
