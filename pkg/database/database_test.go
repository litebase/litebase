package database_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
)

func TestDatabaseCanBeCreated(t *testing.T) {
	app, _ := test.Setup(t)

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
