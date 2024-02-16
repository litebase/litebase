package file

import (
	"io"
	"litebasedb/internal/config"
	"log"
)

type PageCacheReader struct {
	r         io.Reader
	pageCache *PageCache
	offset    int64
}

func (cr *PageCacheReader) Read(p []byte) (n int, err error) {
	n, err = cr.r.Read(p)
	if err != nil {
		log.Println("READ FROM FILE", err)
		// log.Println("READ FROM FILE", cr.offset, n, err)
		return n, err
	}

	// Write the data to the cache.
	// This assumes that the data fits into a single page.
	// If that's not the case, you'll need to adjust this code.
	if n == int(config.Get().PageSize) {
		cr.pageCache.Put(cr.offset, p)
	}

	cr.offset += int64(n)

	return n, err
}
