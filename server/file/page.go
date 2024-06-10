package file

func PageNumber(offset, pageSize int64) int64 {
	return (offset / pageSize) + 1
}

// Calculate the offset of the page within the file
func PageOffset(pageNumber, pageSize int64) int64 {
	return (pageNumber - 1) * pageSize
}
