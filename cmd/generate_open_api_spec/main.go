package main

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"

	"github.com/litebase/litebase/pkg/openapi"
)

func main() {
	log.Printf("Starting dynamic OpenAPI generation with route reflection...")

	// Initialize the dynamic analyzer
	analyzer := openapi.NewGenerator()
	log.Printf("Dynamic analyzer initialized")

	// Use the route reflection method to analyze all routes automatically
	log.Printf("Analyzing all routes using router reflection...")
	combinedAnalysis, err := analyzer.AnalyzeAllRoutes()
	if err != nil {
		log.Fatalf("Failed to analyze routes: %v", err)
	}
	log.Printf("Route analysis complete, found %d methods", len(combinedAnalysis.Methods))

	// Generate OpenAPI paths from combined analysis
	log.Printf("Generating OpenAPI paths from combined analysis...")
	pathItems := convertToPathItems(analyzer.GenerateOpenAPIFromAnalysis(combinedAnalysis))
	log.Printf("Generated %d path items", len(pathItems))

	// Generate full OpenAPI spec
	spec := &openapi.OpenAPISpec{
		OpenAPI: "3.1.0",
		Info: openapi.Info{
			Title:       "Litebase Server API",
			Description: "Litebase Server OpenAPI specification",
			Version:     "1.0.0",
		},
		Servers: []openapi.Server{
			{
				URL:         "http://localhost:8080",
				Description: "Development server",
			},
		},
		Paths: pathItems,
		Components: &openapi.Components{
			SecuritySchemes: openapi.GetSecuritySchemes(),
			Schemas:         openapi.GetCommonSchemas(),
		},
		Tags: openapi.GetTags(),
	}

	// Convert to JSON
	log.Printf("Converting spec to JSON...")
	jsonOutput, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		log.Fatalf("Failed to marshal OpenAPI spec: %v", err)
	}
	log.Printf("JSON conversion complete, output size: %d bytes", len(jsonOutput))

	// Write to file
	outputPath := filepath.Join("api", "openapi.json")
	log.Printf("Writing to file: %s", outputPath)
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	err = os.WriteFile(outputPath, jsonOutput, 0644)
	if err != nil {
		log.Fatalf("Failed to write OpenAPI spec to file: %v", err)
	}

	log.Printf("OpenAPI generation complete! File written to %s", outputPath)
}

func convertToPathItems(paths map[string]map[string]*openapi.Operation) map[string]openapi.PathItem {
	pathItems := make(map[string]openapi.PathItem)

	for path, methods := range paths {
		pathItem := openapi.PathItem{}

		for method, operation := range methods {
			switch method {
			case "get":
				pathItem.Get = operation
			case "post":
				pathItem.Post = operation
			case "put":
				pathItem.Put = operation
			case "delete":
				pathItem.Delete = operation
			case "patch":
				pathItem.Patch = operation
			}
		}

		pathItems[path] = pathItem
	}

	return pathItems
}
