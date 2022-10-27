package database

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"log"
	"sort"
)

type DatabaseWAL struct {
	ChangedPages    map[int][]byte
	databasePath    string
	dirtyPages      map[int64]*Page
	headerHash      string
	isCheckPointing bool
	pageSize        int
}

type Page struct {
	offset int64
	data   []byte
}

func NewWAL(path string) *DatabaseWAL {
	return &DatabaseWAL{
		databasePath:    path,
		isCheckPointing: false,
		dirtyPages:      make(map[int64]*Page),
		pageSize:        4096,
	}
}

func (w *DatabaseWAL) CheckPoint() {
	if len(w.dirtyPages) == 0 {
		return
	}

	w.ChangedPages = make(map[int][]byte)

	w.isCheckPointing = true

	file, _, err := LitebaseDBVFS.Open(w.databasePath, 0)

	if err != nil {
		log.Fatal(err)
	}

	keys := make([]int, 0, len(w.dirtyPages))

	for k := range w.dirtyPages {
		keys = append(keys, int(k))
	}

	sort.Ints(keys)

	for _, key := range keys {
		page := w.dirtyPages[int64(key)]
		w.ChangedPages[key] = page.data
		_, err := file.WriteAt(page.data, page.offset)

		if err != nil {
			log.Fatal(err)
		}
	}

	w.dirtyPages = make(map[int64]*Page)
	w.isCheckPointing = false
}

func (w *DatabaseWAL) CheckPointing() bool {
	return w.isCheckPointing
}

func (w *DatabaseWAL) createHeaderHash(header []byte) string {
	hash := sha1.New()
	hash.Write(header)
	return hex.EncodeToString(hash.Sum(nil))
}

func (w *DatabaseWAL) HasPage(offset int64) bool {
	pageNumber := offset / int64(w.pageSize)

	if _, ok := w.dirtyPages[pageNumber]; ok {
		return true
	}

	return false
}

func (w *DatabaseWAL) HeaderEmpty() bool {
	return w.headerHash == ""
}

func (w *DatabaseWAL) ReadAt(data []byte, offset int64) (int, error) {
	fmt.Println("READING from WAL")
	pageNumber := offset / int64(w.pageSize)

	copy(data, w.dirtyPages[pageNumber].data)

	return len(data), nil
}

func (w *DatabaseWAL) SetHeaderHash(hash []byte) {
	sum := 0

	for _, v := range hash {
		sum += int(v)
	}

	if len(hash) == 0 || sum == 0 {
		w.headerHash = ""
		return
	}

	w.headerHash = w.createHeaderHash(hash)
}

func (w *DatabaseWAL) WriteAt(data []byte, offset int64) (int, error) {
	pageNumber := offset / int64(w.pageSize)

	w.dirtyPages[pageNumber] = &Page{
		data:   data,
		offset: offset,
	}

	return len(data), nil
}
