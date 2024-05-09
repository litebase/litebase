package storage

import (
	"log"
	"sync"
)

type PageReader struct {
	fs DatabaseFileSystem
}

func NewPageReader(fs DatabaseFileSystem) *PageReader {
	return &PageReader{
		fs: fs,
	}
}

func (pr *PageReader) ReadAhead(name string, pageNumber, offset int64, data []byte) {
	page := NewDatabasePage(pageNumber, data)

	if page.PageType() == BTreeInteriorPage {
		log.Println("BTreeInteriorPage")

		pages := page.ChildPages()
		pageBatches := [][]int{}
		batchSize := 100
		batchIndex := 0

		for _, p := range pages {
			if pr.fs.PageCache().Has(int64(p) * pr.fs.PageSize()) {
				continue
			}

			if len(pageBatches) == 0 {
				pageBatches = append(pageBatches, []int{})
			}

			if len(pageBatches[batchIndex]) == batchSize {
				pageBatches = append(pageBatches, []int{})
				batchIndex++
			}

			pageBatches[batchIndex] = append(pageBatches[batchIndex], p)
		}

		for _, batch := range pageBatches {
			wg := sync.WaitGroup{}

			for _, p := range batch {
				wg.Add(1)

				go func(p int) {
					defer wg.Done()

					data, err := pr.fs.FetchPage(int64(p))

					if err != nil {
						log.Println("Error reading ahead", p, err)
						return
					}

					if len(data) == int(pr.fs.PageSize()) {
						// log.Println("Putting page in cache", p)
						pr.fs.PageCache().Put(offset, data)
					}
				}(p)
			}

			wg.Wait()
		}
	}
}
