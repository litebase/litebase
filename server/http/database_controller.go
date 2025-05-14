package http

import (
	"fmt"
	"log"
)

type DatabaseStoreRequest struct {
	Name string `json:"name" validate:"required"`
}

func DatabaseIndexController(request *Request) Response {
	dbs, err := request.databaseManager.All()

	if err != nil {
		return ServerErrorResponse(err)
	}

	return SuccessResponse(
		"Successfully retrieved databases.",
		dbs,
		200,
	)
}

func DatabaseShowController(request *Request) Response {
	database_id := request.Param("database_id")

	if database_id == "" {
		return BadRequestResponse(fmt.Errorf("a valid database_id is required"))
	}

	db, err := request.databaseManager.Get(database_id)

	if err != nil {
		log.Println(err)
		return BadRequestResponse(err)
	}

	return SuccessResponse(
		"Successfully retrieved database.",
		db,
		200,
	)
}

func DatabaseStoreController(request *Request) Response {
	input, err := request.Input(&DatabaseStoreRequest{})

	if err != nil {
		log.Println(err)
		return BadRequestResponse(err)
	}

	validationErrors := request.Validate(input, map[string]string{
		"name.required": "The name field is required.",
	})

	if validationErrors != nil {
		log.Println(validationErrors)
		return ValidationErrorResponse(validationErrors)
	}

	var databaseName = input.(*DatabaseStoreRequest).Name

	// check if the database exists
	exists, err := request.databaseManager.Exists(databaseName)

	if err != nil {
		log.Println(err)
		return ServerErrorResponse(err)
	}

	if exists {
		return BadRequestResponse(fmt.Errorf("database '%s' already exists", databaseName))
	}

	db, err := request.databaseManager.Create(
		databaseName,
		request.cluster.Config.DefaultBranchName,
	)

	if err != nil {
		return ServerErrorResponse(err)
	}

	request.databaseManager.Get(db.Id)

	return SuccessResponse(
		"Database created successfully.",
		db,
		200,
	)
}

func DatabaseDestroyController(request *Request) Response {
	db, err := request.databaseManager.Get(request.Param("databaseId"))

	if err != nil {
		return BadRequestResponse(err)
	}

	err = request.databaseManager.Delete(db)

	if err != nil {
		log.Println(err)
		return ServerErrorResponse(err)
	}

	return SuccessResponse(
		"Database deleted successfully.",
		map[string]any{},
		200,
	)
}
