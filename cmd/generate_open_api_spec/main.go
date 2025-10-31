package main

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/litebase/litebase/pkg/openapi"
)

// combineSchemas merges multiple schema maps into one, preferring more detailed schemas
func combineSchemas(schemaMaps ...map[string]*openapi.Schema) map[string]*openapi.Schema {
	result := make(map[string]*openapi.Schema)

	for _, schemaMap := range schemaMaps {
		for name, schema := range schemaMap {
			// Check if we already have a schema with this name
			if existing, exists := result[name]; exists {
				// If the new schema is more detailed (has properties) and the existing one doesn't,
				// or if it's a qualified name (contains .) vs unqualified, prefer the new one
				if shouldPreferSchema(schema, existing, name) {
					result[name] = schema
				}
				// Otherwise keep the existing one
			} else {
				// No conflict, just add it
				result[name] = schema
			}
		}
	}

	// Handle qualified vs unqualified name conflicts
	// If we have both "auth.Statement" and "Statement", prefer the simple name with detailed content
	for qualifiedName, qualifiedSchema := range result {
		if strings.Contains(qualifiedName, ".") {
			parts := strings.Split(qualifiedName, ".")

			if len(parts) == 2 {
				simpleName := parts[1]

				if simpleSchema, exists := result[simpleName]; exists {
					// If qualified schema is more detailed, use it for the simple name and remove qualified
					if shouldPreferSchema(qualifiedSchema, simpleSchema, qualifiedName) {
						result[simpleName] = qualifiedSchema
						delete(result, qualifiedName)
					} else {
						// If simple schema is better or equal, just remove the qualified one
						delete(result, qualifiedName)
					}
				} else {
					// No simple version exists, add the qualified schema under the simple name
					result[simpleName] = qualifiedSchema
					delete(result, qualifiedName)
				}
			}
		}
	}

	return result
}

// shouldPreferSchema determines if newSchema should be preferred over existing schema
func shouldPreferSchema(newSchema, existingSchema *openapi.Schema, name string) bool {
	// Prefer schemas with properties over those without
	newHasProps := len(newSchema.Properties) > 0
	existingHasProps := len(existingSchema.Properties) > 0

	// Also consider schemas with composition (OneOf, AnyOf, AllOf) as having structure
	newHasComposition := len(newSchema.OneOf) > 0 || len(newSchema.AnyOf) > 0 || len(newSchema.AllOf) > 0
	existingHasComposition := len(existingSchema.OneOf) > 0 || len(existingSchema.AnyOf) > 0 || len(existingSchema.AllOf) > 0

	// Also consider schemas with enums as having structure
	newHasEnum := len(newSchema.Enum) > 0
	existingHasEnum := len(existingSchema.Enum) > 0

	newHasStructure := newHasProps || newHasComposition || newHasEnum
	existingHasStructure := existingHasProps || existingHasComposition || existingHasEnum

	if newHasStructure && !existingHasStructure {
		return true
	}

	if !newHasStructure && existingHasStructure {
		return false
	}

	// If both have properties or both don't, prefer qualified names (contain ".")
	newIsQualified := strings.Contains(name, ".")

	return newIsQualified
}

// fixSchemaReferences updates all $ref references to use simplified schema names
func fixSchemaReferences(spec *openapi.OpenAPISpec) {
	// Create a mapping of qualified names to simple names
	// We need to map all possible qualified references to their simple versions
	refMap := make(map[string]string)

	// Look for simple schema names that exist
	for schemaName := range spec.Components.Schemas {
		if !strings.Contains(schemaName, ".") {
			// This is a simple name, create mapping from qualified versions
			refMap["#/components/schemas/auth."+schemaName] = "#/components/schemas/" + schemaName
			refMap["#/components/schemas/http."+schemaName] = "#/components/schemas/" + schemaName
			refMap["#/components/schemas/config."+schemaName] = "#/components/schemas/" + schemaName
		}
	}

	// Update references in all paths
	for _, pathItem := range spec.Paths {
		fixOperationReferences(pathItem.Get, refMap)
		fixOperationReferences(pathItem.Post, refMap)
		fixOperationReferences(pathItem.Put, refMap)
		fixOperationReferences(pathItem.Delete, refMap)
		fixOperationReferences(pathItem.Patch, refMap)
	}

	// Update references in schemas themselves
	for _, schema := range spec.Components.Schemas {
		fixSchemaReferencesRecursive(schema, refMap)
	}
}

// fixOperationReferences fixes references in an operation
func fixOperationReferences(op *openapi.Operation, refMap map[string]string) {
	if op == nil {
		return
	}

	// Fix request body references
	if op.RequestBody != nil && op.RequestBody.Content != nil {
		for _, mediaType := range op.RequestBody.Content {
			if mediaType.Schema != nil {
				fixSchemaReferencesRecursive(mediaType.Schema, refMap)
			}
		}
	}

	// Fix response references
	for _, response := range op.Responses {
		if response.Content != nil {
			for _, mediaType := range response.Content {
				if mediaType.Schema != nil {
					fixSchemaReferencesRecursive(mediaType.Schema, refMap)
				}
			}
		}
	}
}

// fixSchemaReferencesRecursive recursively fixes references in a schema
func fixSchemaReferencesRecursive(schema *openapi.Schema, refMap map[string]string) {
	if schema == nil {
		return
	}

	// Fix direct reference
	if schema.Ref != "" {
		if newRef, exists := refMap[schema.Ref]; exists {
			schema.Ref = newRef
		}
	}

	// Fix array item references
	if schema.Items != nil {
		fixSchemaReferencesRecursive(schema.Items, refMap)
	}

	// Fix property references
	for _, prop := range schema.Properties {
		fixSchemaReferencesRecursive(prop, refMap)
	}
}

func main() {
	// Initialize the dynamic analyzer
	analyzer := openapi.NewGenerator()

	// Use the route reflection method to analyze all routes automatically
	combinedAnalysis, err := analyzer.AnalyzeAllRoutes()

	if err != nil {
		log.Fatalf("Failed to analyze routes: %v", err)
	}

	log.Printf("Route analysis complete, found %d methods", len(combinedAnalysis.Methods))

	// Generate OpenAPI paths from combined analysis
	pathItems := convertToPathItems(analyzer.GenerateOpenAPIFromAnalysis(combinedAnalysis))

	log.Printf("Generated %d path items", len(pathItems))

	// Collect all tags used in the path items for dynamic tag generation
	usedTags := make(map[string]bool)

	for _, pathItem := range pathItems {
		for _, operation := range []*openapi.Operation{
			pathItem.Get, pathItem.Post, pathItem.Put, pathItem.Delete, pathItem.Patch,
		} {
			if operation != nil {
				for _, tag := range operation.Tags {
					usedTags[tag] = true
				}
			}
		}
	}

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
			Schemas:         combineSchemas(openapi.GetCommonSchemas(), filterInternalSchemas(analyzer.GetRegisteredSchemas())),
		},
		Tags: openapi.GenerateDynamicTags(usedTags),
	}

	// Fix broken references after schema deduplication
	fixSchemaReferences(spec)

	// Convert to JSON
	jsonOutput, err := json.MarshalIndent(spec, "", "  ")

	if err != nil {
		log.Fatalf("Failed to marshal OpenAPI spec: %v", err)
	}

	log.Printf("JSON conversion complete, output size: %d bytes", len(jsonOutput))

	// Write to file
	outputPath := filepath.Join("api", "generated_open_api.json")

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

// filterInternalSchemas removes internal/stdlib schemas that shouldn't be exposed in the API spec
// This includes: cgo types (_Ctype*), stdlib types (Context, Connection), and internal helper types
func filterInternalSchemas(in map[string]*openapi.Schema) map[string]*openapi.Schema {
	out := make(map[string]*openapi.Schema)

	// List of type names (simple or qualified) that should be excluded
	excludedTypes := map[string]bool{
		"Context":            true, // context.Context from stdlib
		"Connection":         true, // net.Conn or similar - if used only as header, not needed as schema
		"StatementReadonly":  true, // internal sqlite3 helper type
		"Column":             true, // sqlite3.Column - unused in API responses (ColumnValue is used instead)
		"Authorizer":         true, // sqlite3.Authorizer - internal function type for cgo callbacks
		"Handle":             true, // cgo.Handle - internal cgo type, not exposed in API
		"error":              true, // builtin error type that got analyzed
		// Add more as needed
	}

	for k, v := range in {
		// If the simple name (after last dot) starts with _Ctype, skip it
		simple := k
		if strings.Contains(k, ".") {
			parts := strings.Split(k, ".")
			simple = parts[len(parts)-1]
		}

		// Skip cgo internal types
		if strings.HasPrefix(k, "_Ctype") || strings.HasPrefix(simple, "_Ctype") {
			continue
		}

		// Skip explicitly excluded types
		if excludedTypes[k] || excludedTypes[simple] {
			continue
		}

		out[k] = v
	}

	return out
}

func convertToPathItems(paths map[string]map[string]*openapi.Operation) map[string]openapi.PathItem {
	pathItems := make(map[string]openapi.PathItem)

	for path, methods := range paths {
		pathItem := openapi.PathItem{}

		// Extract path parameters from all operations and move to path level
		pathParameters := make(map[string]openapi.Parameter)

		// First pass: collect all path parameters from operations
		for _, operation := range methods {
			if operation != nil {
				var nonPathParams []openapi.Parameter

				for _, param := range operation.Parameters {
					if param.In == "path" {
						pathParameters[param.Name] = param
					} else {
						nonPathParams = append(nonPathParams, param)
					}
				}

				// Remove path parameters from operation-level parameters
				operation.Parameters = nonPathParams
			}
		}

		// Convert path parameters map to slice for path item
		for _, param := range pathParameters {
			pathItem.Parameters = append(pathItem.Parameters, param)
		}

		// Sort path-level parameters by their position in the path
		// This ensures parameters appear in the order they're defined in the path
		sort.Slice(pathItem.Parameters, func(i, j int) bool {
			// Find the position of each parameter in the path string
			posI := strings.Index(path, "{"+pathItem.Parameters[i].Name+"}")
			posJ := strings.Index(path, "{"+pathItem.Parameters[j].Name+"}")
			return posI < posJ
		})

		// Second pass: assign operations to path item
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
