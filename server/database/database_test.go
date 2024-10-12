package database_test

import (
	"litebase/internal/test"
	"testing"
)

func TestDatabaseCanBeCreated(t *testing.T) {
	app := test.Setup(t)

	databaseId := "test"
	branchId := "test"

	db, err := app.DatabaseManager.Create(
		databaseId,
		branchId,
	)

	if db == nil {
		t.Fail()
	}

	if err != nil {
		t.Error(err)
	}
}
