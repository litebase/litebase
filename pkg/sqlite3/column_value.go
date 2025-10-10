package sqlite3

type ColumnValue struct {
	Column *Column
}

func NewColumnValue(column *Column) *ColumnValue {
	return &ColumnValue{
		Column: column,
	}
}

func (cv *ColumnValue) MarshalJSON() ([]byte, error) {
	return cv.Column.MarshalJSON()
}
