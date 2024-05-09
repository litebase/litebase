package storage

import (
	"encoding/binary"
	"log"
)

const (
	BTreeInteriorPage      = 0x05
	BTreeLeafPage          = 0x0D
	IndexBTreeInteriorPage = 0x02
	IndexBTreeLeafPage     = 0x0A
	TableBTreePage         = 0
)

type DatabasePage struct {
	Data       []byte
	PageNumber int64
}

func NewDatabasePage(pageNumber int64, data []byte) DatabasePage {
	return DatabasePage{
		Data:       data,
		PageNumber: pageNumber,
	}
}

func (dp DatabasePage) PageType() int {
	if dp.PageNumber == 1 {
		return TableBTreePage
	}

	switch dp.Data[0] {
	case BTreeInteriorPage:
		return BTreeInteriorPage
	case BTreeLeafPage:
		return BTreeLeafPage
	case IndexBTreeInteriorPage:
		return IndexBTreeInteriorPage
	case IndexBTreeLeafPage:
		return IndexBTreeLeafPage
	default:
		return -1
	}
}

func (dp DatabasePage) ChildPages() []int {
	// The header is 12 bytes long
	d := dp.Data
	header := d[:12]
	cellOffset := 12

	// Parse cell pointers
	numCells := binary.BigEndian.Uint16(dp.Data[3:5])
	log.Println("Number of Cells:", numCells)

	pages := []int{}

	for i := 0; i < int(numCells); i++ {
		cellPointerOffset := cellOffset + (i * 2)
		cellPointer := dp.Data[cellPointerOffset : cellPointerOffset+2]

		// Extract child page number
		// Extract child page number
		childPageOffset := binary.BigEndian.Uint16(cellPointer)
		childPageNumber := binary.BigEndian.Uint32(header[childPageOffset : childPageOffset+4])
		// fmt.Printf("Child Page Number: %d\n", childPageNumber)

		pages = append(pages, int(childPageNumber))
	}

	return pages
}
