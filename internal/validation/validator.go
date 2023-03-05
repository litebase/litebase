package validation

import (
	"reflect"

	"github.com/go-playground/validator"
)

func Validate(input interface{}) validator.ValidationErrors {
	err := validator.New().Struct(input)

	if err != nil {
		if reflect.TypeOf(err) == reflect.TypeOf(validator.ValidationErrors{}) {
			return err.(validator.ValidationErrors)
		}
	}

	return nil
}
