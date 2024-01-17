package validation

import (
	"reflect"
	"strings"

	"github.com/go-playground/validator"
)

func Validate(input interface{}) validator.ValidationErrors {
	v := validator.New()

	// register function to get tag name from json tags.
	v.RegisterTagNameFunc(func(fld reflect.StructField) string {
		name := strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]

		if name == "_" {
			return ""
		}
		return name
	})

	err := v.Struct(input)

	if err != nil {
		if reflect.TypeOf(err) == reflect.TypeOf(validator.ValidationErrors{}) {
			return err.(validator.ValidationErrors)
		}
	}

	return nil
}
