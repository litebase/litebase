package query

import (
	"litebasedb/runtime/app/database"
	"litebasedb/runtime/internal/test"
	"log"
	"testing"
)

func TestNewQuery(t *testing.T) {
	test.Run(func() {
		mock := test.MockDatabase()

		db, err := database.Get(mock["databaseUuid"], mock["branchUuid"], nil, false)

		if err != nil {
			log.Fatal(err)
		}

		encrytpedQuery := test.EncryptQuery(
			"SELECT * FROM users LIMIT ?",
			"[1]",
			mock["accessKeyId"],
			mock["accessKeySecret"],
		)

		query, err := NewQuery(db, mock["accessKeyId"], map[string]interface{}{
			"statement":  encrytpedQuery["statement"],
			"parameters": encrytpedQuery["parameters"],
		}, "123")

		if err != nil {
			t.Fatal(err)
		}

		if query.JsonStatement != "SELECT * FROM users LIMIT ?" {
			t.Fatal("Statement is not correct")
		}
	})
}

func TestNewQueryWithInvalidaAccessKeyId(t *testing.T) {
	test.Run(func() {
		mock := test.MockDatabase()

		db, err := database.Get(mock["databaseUuid"], mock["branchUuid"], nil, false)

		if err != nil {
			log.Fatal(err)
		}

		encrytpedQuery := test.EncryptQuery(
			"SELECT * FROM users LIMIT ?",
			"[1]",
			mock["accessKeyId"],
			mock["accessKeySecret"],
		)

		_, err = NewQuery(db, "invalid", map[string]interface{}{
			"statement":  encrytpedQuery["statement"],
			"parameters": encrytpedQuery["parameters"],
		}, "")

		if err == nil {
			t.Fatal("Error should not be nil")
		}
	})
}

func TestNewQueryWithStatementThatCannotBeDecrypted(t *testing.T) {
	test.Run(func() {
		mock := test.MockDatabase()

		db, err := database.Get(mock["databaseUuid"], mock["branchUuid"], nil, false)

		if err != nil {
			log.Fatal(err)
		}

		_, err = NewQuery(db, mock["accessKeyId"], map[string]interface{}{
			"statement":  "statement",
			"parameters": "parameters",
		}, "")

		if err == nil {
			t.Fatal("Error should not be nil")
		}
	})
}

func TestNewQueryWithParametersThatCannotBeDecrypted(t *testing.T) {
	test.Run(func() {
		mock := test.MockDatabase()

		db, err := database.Get(mock["databaseUuid"], mock["branchUuid"], nil, false)

		if err != nil {
			log.Fatal(err)
		}

		encrytpedQuery := test.EncryptQuery(
			"SELECT * FROM users LIMIT ?",
			"[1]",
			mock["accessKeyId"],
			mock["accessKeySecret"],
		)

		_, err = NewQuery(db, mock["accessKeyId"], map[string]interface{}{
			"statement":  encrytpedQuery["statement"],
			"parameters": "parameters",
		}, "")

		if err == nil {
			t.Fatal("Error should not be nil")
		}
	})
}

func TestResolve(t *testing.T) {
	test.Run(func() {
		mock := test.MockDatabase()
		db, _ := database.Get(mock["databaseUuid"], mock["branchUuid"], nil, false)

		test.RunQuery(db, "CREATE TABLE users (id INT, name TEXT)", []interface{}{})

		db, _ = database.Get(mock["databaseUuid"], mock["branchUuid"], nil, false)

		encrytpedQuery := test.EncryptQuery(
			"SELECT * FROM users LIMIT ?",
			"[1]",
			mock["accessKeyId"],
			mock["accessKeySecret"],
		)

		query, err := NewQuery(db, mock["accessKeyId"], map[string]interface{}{
			"statement":  encrytpedQuery["statement"],
			"parameters": encrytpedQuery["parameters"],
		}, "")

		if err != nil {
			t.Fatal(err)
		}

		response := query.Resolve()

		if response["status"] != "success" {
			t.Fatal("Response status is not correct")
		}
	})
}

func TestStatement(t *testing.T) {
	test.Run(func() {
		mock := test.MockDatabase()
		db, _ := database.Get(mock["databaseUuid"], mock["branchUuid"], nil, false)

		test.RunQuery(db, "CREATE TABLE users (id INT, name TEXT)", []interface{}{})

		db, _ = database.Get(mock["databaseUuid"], mock["branchUuid"], nil, false)

		query := &Query{
			Database:       db,
			JsonStatement:  "SELECT * FROM users LIMIT ?",
			JsonParameters: "[1]",
		}

		statement, err := query.Statement()

		if err != nil {
			t.Fatal(err)
		}

		if statement.SQL() != "SELECT * FROM users LIMIT ?" {
			t.Fatal("Statement is not correct")
		}
	})
}

func TestStatementOfBatchQuery(t *testing.T) {
	test.Run(func() {
		mock := test.MockDatabase()
		db, _ := database.Get(mock["databaseUuid"], mock["branchUuid"], nil, false)

		test.RunQuery(db, "CREATE TABLE users (id INT, name TEXT)", []interface{}{})

		db, _ = database.Get(mock["databaseUuid"], mock["branchUuid"], nil, false)

		query := &Query{
			Batch: []*Query{{
				Database:       db,
				JsonStatement:  "SELECT * FROM users LIMIT ?",
				JsonParameters: "[1]",
			}},
			Database: db,
		}

		statement, err := query.Statement()

		if err != nil {
			t.Fatal(err)
		}

		if statement != nil {
			t.Fatal("Statement should be nil for a query with the batch field")
		}
	})
}

func TestValidate(t *testing.T) {
	test.Run(func() {
		mock := test.MockDatabase()
		db, _ := database.Get(mock["databaseUuid"], mock["branchUuid"], nil, false)

		test.RunQuery(db, "CREATE TABLE users (id INT, name TEXT)", []interface{}{})

		db, _ = database.Get(mock["databaseUuid"], mock["branchUuid"], nil, false)

		query := &Query{
			Database:       db,
			JsonStatement:  "SELECT * FROM users LIMIT ?",
			JsonParameters: "[1]",
		}

		err := query.Validate()

		if err != nil {
			t.Fatal(err)
		}
	})
}
