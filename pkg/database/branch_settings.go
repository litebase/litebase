package database

type BranchSettings struct {
	IncrementableBackups bool `json:"incrementable_backups"`
}

// // database/sql Scanner interface
// func (b *BranchSettings) Scan(value any) error {
// 	if value == nil {
// 		return nil
// 	}

// 	return nil
// }

// // database/sql Valuer interface
// func (b *BranchSettings) Value() (any, error) {
// 	log.Fatal("BranchSettings.Value() is not implemented yet")
// 	jsonData, err := json.Marshal(b)

// 	if err != nil {
// 		return nil, err
// 	}

// 	return jsonData, nil
// }
