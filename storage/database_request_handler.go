package storage

import (
	"fmt"
	"log"
	"os"
)

var files = map[string]*os.File{}

type DatabaseRequest struct {
	Action       string `json:"action"`
	DatabaseName string `json:"database_name"`
	Pages        []Page `json:"pages"`
	Id           string `json:"id"`
}

type DatabaseResponse struct {
	Id    string `json:"id"`
	Size  int64  `json:"size"`
	Pages []Page `json:"pages"`
}

type Page struct {
	Offset int64  `json:"offset"`
	Length int64  `json:"length"`
	Data   []byte `json:"data"`
	Error  string `json:"error"`
}

func DatabaseRequestHandler(id string, request DatabaseRequest) (DatabaseResponse, error) {
	var response DatabaseResponse
	request.Id = id

	// TODO - return proper error
	if request.DatabaseName == "" {
		return DatabaseResponse{}, nil
	}

	if err := open(request.DatabaseName); err != nil {
		return DatabaseResponse{}, nil
	}

	if request.Action == "read" {
		response.Pages = read(request.DatabaseName, request.Pages)
	}

	if request.Action == "write" {
		response.Pages = write(request.DatabaseName, request.Pages)
	}

	if request.Action == "size" || request.Action == "write" {
		size, err := getSize(request.DatabaseName)

		if err != nil {
			return DatabaseResponse{
				Size:  0,
				Id:    request.Id,
				Pages: []Page{},
			}, nil
		}

		response.Size = size
	}

	response.Id = request.Id

	return response, nil
}

func open(databaseName string) error {
	if ok := files[databaseName]; ok == nil {
		path := fmt.Sprintf("%s/%s.db", os.Getenv("DATABASE_DIRECTORY"), databaseName)
	open:
		file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)

		if err != nil && err == os.ErrNotExist || os.IsNotExist(err) {
			err := os.MkdirAll(os.Getenv("DATABASE_DIRECTORY"), 0755)

			if err != nil {
				log.Println(err)

				return err
			}

			goto open
		} else if err != nil {
			log.Println(err)

			return err
		}

		files[databaseName] = file
	}

	return nil
}

func getSize(databaseName string) (int64, error) {
	fileInfo, err := files[databaseName].Stat()

	if err != nil {
		log.Println(err)

		return 0, err
	}

	return fileInfo.Size(), nil
}

func read(databaseName string, pages []Page) []Page {
	readPages := []Page{}

	for _, page := range pages {
		pageData := make([]byte, page.Length)
		n, err := files[databaseName].ReadAt(pageData, page.Offset)
		var errorMessage string

		if err != nil {
			errorMessage = err.Error()
		}

		readPages = append(readPages, Page{
			Offset: page.Offset,
			Length: int64(n),
			Data:   pageData,
			Error:  errorMessage,
		})
	}

	return readPages

}

func write(databaseName string, pages []Page) []Page {
	writtenPages := []Page{}
	for _, page := range pages {
		n, err := files[databaseName].WriteAt(page.Data, page.Offset)

		if err != nil {
			log.Println(err)
		}

		var errorMessage string

		if err != nil {
			errorMessage = err.Error()
		}

		writtenPages = append(writtenPages, Page{
			Offset: page.Offset,
			Length: int64(n),
			Error:  errorMessage,
		})
	}

	files[databaseName].Sync()

	return writtenPages
}
