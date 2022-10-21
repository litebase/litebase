package database

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"log"
	"sort"

	"github.com/psanford/sqlite3vfs"
)

type DatabaseWAL struct {
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

	w.isCheckPointing = true

	file, _, err := LitebaseDBVFS.Open(w.databasePath, 0)

	if err != nil {
		log.Fatal(err)
	}

	// header := make([]byte, 100)
	// _, err = file.ReadAt(header, 0)

	// if err != nil && err.Error() != "EOF" {
	// 	log.Fatal(err)
	// }

	// if !w.HeaderEmpty() && !w.HashIsEqual(header) {
	// 	w.RefresHeaderHash(file)
	// 	// TODO: If the database header hash has changed since the change set
	// 	// was created, the checkpoint should be aborted and changes retried
	// 	// on the updated database pages

	// 	log.Fatal("Database has changed since the checkpoint frame")
	// }

	keys := make([]int, 0, len(w.dirtyPages))

	for k := range w.dirtyPages {
		keys = append(keys, int(k))
	}

	sort.Ints(keys)

	for _, key := range keys {
		page := w.dirtyPages[int64(key)]
		_, err := file.WriteAt(page.data, page.offset)

		if err != nil {
			log.Fatal(err)
		}
	}

	// file.Sync(3)

	// w.RefresHeaderHash(file)
	w.dirtyPages = make(map[int64]*Page)
	w.isCheckPointing = false
	// concurrency.Unlock()
}

func (w *DatabaseWAL) CheckPointing() bool {
	return w.isCheckPointing
}

func (w *DatabaseWAL) createHeaderHash(header []byte) string {
	hash := sha1.New()
	hash.Write(header)
	return hex.EncodeToString(hash.Sum(nil))
}

func (w *DatabaseWAL) HashIsEqual(hash []byte) bool {
	return w.headerHash == w.createHeaderHash(hash)
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

func (w *DatabaseWAL) RefresHeaderHash(file sqlite3vfs.File) {
	header := make([]byte, 100)
	_, err := file.ReadAt(header, 0)

	if err != nil {
		log.Fatal(err)
	}

	w.SetHeaderHash(header)
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
