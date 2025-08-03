package validation

import (
	"fmt"
	"log/slog"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/go-playground/validator/v10"
)

var (
	// Use a singleton validator instance to avoid recreating it
	validatorInstance *validator.Validate
	validatorOnce     sync.Once
)

func getValidator() *validator.Validate {
	validatorOnce.Do(func() {
		validatorInstance = validator.New()

		// register function to get tag name from json tags.
		validatorInstance.RegisterTagNameFunc(func(fld reflect.StructField) string {
			name := strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]

			if name == "_" {
				return ""
			}

			return name
		})
	})

	return validatorInstance
}

func Validate(input any, messages map[string]string) map[string][]string {
	v := getValidator()

	err := v.Struct(input)

	if err != nil {
		if reflect.TypeOf(err) == reflect.TypeOf(validator.ValidationErrors{}) {
			err := err.(validator.ValidationErrors)
			var e map[string][]string = make(map[string][]string)

			for _, x := range err {
				fieldKey := x.Field()
				namespace := x.Namespace()
				tag := x.Tag()

				// Check if this is a slice dive validation error
				if namespace != "" && strings.Contains(namespace, "[") && strings.Contains(namespace, "]") {
					// Remove the first part of the namespace which is the struct name
					// e.g. "TestStruct.users[0].email" -> "users[0].email"
					namespace = namespace[strings.Index(namespace, ".")+1:]

					// Convert array index notation to wildcard for message lookup
					wildcardKey := strings.ReplaceAll(namespace, "[", ".")
					wildcardKey = strings.ReplaceAll(wildcardKey, "]", "")

					// Replace numeric indices with wildcards
					parts := strings.Split(wildcardKey, ".")
					partNumbers := []int{}

					for i, part := range parts {
						if number, err := strconv.Atoi(part); err == nil {
							parts[i] = "*"
							partNumbers = append(partNumbers, number)
						}
					}

					wildcardKey = strings.Join(parts, ".")
					messageKey := fmt.Sprintf("%s.%s", wildcardKey, tag)

					if messages[messageKey] == "" {
						slog.Debug("Validation error message not found", "key", messageKey)
						continue
					}

					var result strings.Builder
					numIdx := 0

					for _, ch := range wildcardKey {
						if ch == '*' && numIdx < len(partNumbers) {
							result.WriteString(strconv.Itoa(partNumbers[numIdx]))
							numIdx++
						} else {
							result.WriteRune(ch)
						}
					}

					errorKey := result.String()

					e[errorKey] = append(e[errorKey], messages[messageKey])
				} else {
					messageKey := fmt.Sprintf("%s.%s", fieldKey, tag)
					e[fieldKey] = append(e[fieldKey], messages[messageKey])
				}
			}

			return e
		}
	}

	return nil
}
