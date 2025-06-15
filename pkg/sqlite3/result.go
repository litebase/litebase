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
	buffers []*bytes.Buffer
	columns []*Column
	Columns []string
	Rows    [][]*Column
}

func NewResult() *Result {
	return &Result{
		buffers: []*bytes.Buffer{},
		columns: []*Column{},
		Columns: []string{},
		Rows:    [][]*Column{},
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

	r.Columns = r.Columns[:0]
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
