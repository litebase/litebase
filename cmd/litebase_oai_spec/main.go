package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/litebase/litebase/pkg/http"
	"github.com/litebase/litebase/pkg/openapi"
)

func main() {
	workDir, err := os.Getwd()

	if err != nil {
		log.Fatalf("Failed to get working directory: %v", err)
	}

	// Create a new router and load routes
	router := http.NewRouter()
	http.LoadRoutes(router)

	// Create OpenAPI generator
	generator := openapi.NewGenerator()

	// Convert routes to the format expected by the generator
	routes := make(map[string]map[string]any)

	for method, methodRoutes := range router.GetRoutes() {
		routes[method] = make(map[string]any)

		for path, route := range methodRoutes {
			routes[method][path] = route
		}
	}

	// Generate OpenAPI spec
	err = generator.GenerateFromRoutes(routes)

	if err != nil {
		log.Fatalf("Failed to generate OpenAPI spec: %v", err)
	}

	// Convert to JSON
	specJSON, err := generator.ToJSON(true)

	if err != nil {
		log.Fatalf("Failed to convert spec to JSON: %v", err)
	}

	// Output file path - keep as JSON for now since YAML conversion would need additional dependencies
	outputPath := filepath.Join(workDir, "api", "generated_openapi.json")

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	// Write to file
	err = os.WriteFile(outputPath, specJSON, 0644)

	if err != nil {
		log.Fatalf("Failed to write OpenAPI spec to file: %v", err)
	}

	fmt.Printf("OpenAPI specification generated successfully!\n")
	fmt.Printf("Output: %s\n", outputPath)

	// Also print basic stats
	var spec openapi.OpenAPISpec

	if err := json.Unmarshal(specJSON, &spec); err == nil {
		fmt.Printf("Generated spec contains:\n")
		fmt.Printf("- %d paths\n", len(spec.Paths))

		operationCount := 0

		for _, pathItem := range spec.Paths {
			if pathItem.Get != nil {
				operationCount++
			}

			if pathItem.Post != nil {
				operationCount++
			}

			if pathItem.Put != nil {
				operationCount++
			}

			if pathItem.Delete != nil {
				operationCount++
			}

			if pathItem.Patch != nil {
				operationCount++
			}

			if pathItem.Options != nil {
				operationCount++
			}

			if pathItem.Head != nil {
				operationCount++
			}
		}

		fmt.Printf("- %d operations\n", operationCount)
		fmt.Printf("- %d tags\n", len(spec.Tags))

		if spec.Components != nil && spec.Components.Schemas != nil {
			fmt.Printf("- %d schemas\n", len(spec.Components.Schemas))
		}
	}
}
