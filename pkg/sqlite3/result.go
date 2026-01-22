package sqlite3

import (
	"bytes"
	"sync"
)

var resultBufferPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, 1024))
	},
}

var resultColumnPool = &sync.Pool{
	New: func() any {
		return &Column{}
	},
}

type Result struct {
	buffers     []*bytes.Buffer
	columns     []*Column
	rowPool     [][]*Column
	Columns     []string
	ColumnTypes []ColumnType
	Rows        [][]*Column
}

func NewResult() *Result {
	return &Result{
		buffers:     []*bytes.Buffer{},
		columns:     []*Column{},
		Columns:     []string{},
		ColumnTypes: []ColumnType{},
		Rows:        [][]*Column{},
	}
}

func (r *Result) ColumnNames() []string {
	return r.Columns
}

func (r *Result) GetBuffer() *bytes.Buffer {
	buffer := resultBufferPool.Get().(*bytes.Buffer)
	r.buffers = append(r.buffers, buffer)

	return buffer
}

func (r *Result) GetColumn() *Column {
	column := resultColumnPool.Get().(*Column)
	r.columns = append(r.columns, column)

	return column
}

// GetRowSlice returns a reusable row slice of the requested length.
// It reuses slices from an internal pool to avoid allocating a new
// []*Column slice for every row.
func (r *Result) GetRowSlice(n int) []*Column {
	// Try to pop from pool
	l := len(r.rowPool)

	if l > 0 {
		row := r.rowPool[l-1]
		r.rowPool = r.rowPool[:l-1]

		if cap(row) < n {
			// grow if insufficient capacity
			row = make([]*Column, n)
		} else {
			row = row[:n]

			for i := range row {
				row[i] = nil
			}
		}

		return row
	}

	return make([]*Column, n)
}

func (r *Result) PutBuffer(buffer *bytes.Buffer) {
	buffer.Reset()

	for i, b := range r.buffers {
		if b == buffer {
			r.buffers = append(r.buffers[:i], r.buffers[i+1:]...)
			break
		}
	}

	resultBufferPool.Put(buffer)
}

func (r *Result) PutColumn(column *Column) {
	column.Reset()
	resultColumnPool.Put(column)
}

func (r *Result) ReleaseBuffers() {
	for _, buffer := range r.buffers {
		buffer.Reset()
		resultBufferPool.Put(buffer)
	}

	r.buffers = r.buffers[:0]
}

func (r *Result) ReleaseColumns() {
	for _, column := range r.columns {
		column.Reset()
		resultColumnPool.Put(column)
	}

	r.columns = r.columns[:0]
}

func (r *Result) Reset() {
	r.ReleaseBuffers()
	r.ReleaseColumns()

	// Return row slices to pool to allow reuse and reduce allocations.
	// Keep the pool bounded to avoid unbounded memory growth.
	const maxRowPool = 1024

	for _, row := range r.Rows {
		if len(r.rowPool) < maxRowPool {
			r.rowPool = append(r.rowPool, row)
		}
	}

	r.Columns = r.Columns[:0]
	r.ColumnTypes = r.ColumnTypes[:0]
	r.Rows = r.Rows[:0]
}

func (r *Result) Row(index int) []*Column {
	if index < 0 || index >= len(r.Rows) {
		return nil
	}

	return r.Rows[index]
}

func (r *Result) RowCount() int {
	return len(r.Rows)
}

func (r *Result) SetColumns(columns []string) {
	if cap(r.Columns) >= len(columns) {
		r.Columns = r.Columns[:0]
	}

	r.Columns = append(r.Columns, columns...)
}
