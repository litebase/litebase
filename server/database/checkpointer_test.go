package database_test

import (
	"litebase/internal/test"
	"litebase/server/database"
	"log"
	"testing"
)

func TestNewCheckpointer(t *testing.T) {
	test.Run(t, func() {
		db := test.MockDatabase()

		cp, err := database.NewCheckpointer(
			database.DatabaseResources().FileSystem(db.DatabaseUuid, db.BranchUuid),
			db.DatabaseUuid,
			db.BranchUuid,
		)

		if err != nil {
			t.Fatal(err)
		}

		if cp == nil {
			t.Fatal("CheckPointer is nil")
		}
	})
}

func TestCheckpointerAddPage(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()
		cp, err := database.NewCheckpointer(
			database.DatabaseResources().FileSystem(mock.DatabaseUuid, mock.BranchUuid),
			mock.DatabaseUuid,
			mock.BranchUuid,
		)

		if err != nil {
			t.Fatal(err)
		}

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

		cp, err := database.NewCheckpointer(
			database.DatabaseResources().FileSystem(mock.DatabaseUuid, mock.BranchUuid),
			mock.DatabaseUuid,
			mock.BranchUuid,
		)

		if err != nil {
			t.Fatal(err)
		}

		cp.AddPage(1)

		err = cp.Run()

		if err != nil {
			t.Fatal(err)
		}
	})
}
