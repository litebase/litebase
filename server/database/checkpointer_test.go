package database_test

import (
	"litebasedb/internal/test"
	"litebasedb/server/database"
	"log"
	"testing"
)

func TestNewCheckpointer(t *testing.T) {
	test.Run(t, func() {
		db := test.MockDatabase()
		cp := database.NewCheckpointer(db.DatabaseUuid, db.BranchUuid)

		if cp == nil {
			t.Fatal("CheckPointer is nil")
		}
	})
}

func TestCheckpointerAddPage(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()
		cp := database.NewCheckpointer(mock.DatabaseUuid, mock.BranchUuid)

		cp.AddPage(1)

		if len(cp.Pages()) != 1 {
			t.Fatal("Page was not added")
		}
	})
}

func TestCheckpointerRun(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		_, err := database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			log.Fatal(err)
		}

		cp := database.NewCheckpointer(mock.DatabaseUuid, mock.BranchUuid)

		cp.AddPage(1)

		err = cp.Run()

		if err != nil {
			t.Fatal(err)
		}
	})
}
