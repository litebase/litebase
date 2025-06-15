package file

// Calculate the page number of the given offset.
func PageNumber(offset, pageSize int64) int64 {
	return (offset / pageSize) + 1
}

// Calculate the offset of the page within the file
func PageOffset(pageNumber, pageSize int64) int64 {
	return (pageNumber - 1) * pageSize
}

// Determine the range of a given page number.
func PageRange(pageNumber, rangeSize int64) int64 {
	return (pageNumber-1)/rangeSize + 1
}

// Calculate the index of the page within the r.
func PageRangeIndex(pageNumber, rangeSize int64) int64 {
	return (pageNumber - 1) % rangeSize
}

// Calculate the offset of the page within the r.
func PageRangeOffset(pageNumber, rangeSize, pageSize int64) int64 {
	return (pageNumber - 1) % rangeSize * pageSize
}

// Calculate the start and end page numbers of the r.
func PageRangeStartAndEndPageNumbers(pageNumber, rangeSize, pageSize int64) (int64, int64) {
	startPageNumber := (pageNumber-1)/rangeSize*rangeSize + 1
	endPageNumber := startPageNumber + rangeSize - 1

	return startPageNumber, endPageNumber
}
