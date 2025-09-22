package openapi

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/litebase/litebase/pkg/http"
)

// Generator performs deep analysis of Go code to extract OpenAPI information
type Generator struct {
	fileSet      *token.FileSet
	typeInfo     map[string]*TypeInfo
	packageCache map[string][]*ast.File // Cache for parsed package files
	importCache  map[string]string      // Cache for import path -> package name mapping
}

// TypeInfo holds information about a Go type for OpenAPI schema generation
type TypeInfo struct {
	Name        string
	Type        string
	Fields      map[string]*FieldInfo
	Description string
	Example     interface{}
}

// FieldInfo holds information about struct fields
type FieldInfo struct {
	Name        string
	Type        string
	JSONName    string
	Required    bool
	Validation  map[string]string
	Description string
	Example     interface{}
}

// ControllerAnalysis holds the complete analysis of a controller
type ControllerAnalysis struct {
	Methods map[string]*MethodAnalysis
}

// MethodAnalysis holds analysis of a single controller method
type MethodAnalysis struct {
	Name        string
	HTTPMethod  string
	Path        string
	Description string
	Parameters  []*ParameterInfo
	RequestBody *RequestBodyInfo
	Responses   map[string]*ResponseInfo
	Security    []string
	Tags        []string
}

// ParameterInfo holds information about method parameters
type ParameterInfo struct {
	Name        string
	In          string // "path", "query", "header"
	Type        string
	Required    bool
	Description string
	Example     interface{}
}

// RequestBodyInfo holds information about request bodies
type RequestBodyInfo struct {
	Type        string
	Schema      *Schema
	Required    bool
	Description string
}

// ResponseInfo holds information about responses
type ResponseInfo struct {
	StatusCode  int
	Description string
	Schema      *Schema
	Type        string
	Message     string // Actual success message extracted from SuccessResponse calls
}

// NewGenerator creates a new generator
func NewGenerator() *Generator {
	return &Generator{
		fileSet:      token.NewFileSet(),
		typeInfo:     make(map[string]*TypeInfo),
		packageCache: make(map[string][]*ast.File),
		importCache:  make(map[string]string),
	}
}

// GetTypeInfo returns the collected type information
func (g *Generator) GetTypeInfo() map[string]*TypeInfo {
	return g.typeInfo
}

// AnalyzeAllRoutes performs comprehensive analysis of all routes and their controllers
func (g *Generator) AnalyzeAllRoutes() (*ControllerAnalysis, error) {
	// Create a router and load all routes
	router := http.NewRouter()
	http.LoadRoutes(router)

	analysis := &ControllerAnalysis{
		Methods: make(map[string]*MethodAnalysis),
	}

	// Track which controller files we've already analyzed and their associated handlers
	analyzedFiles := make(map[string]bool)

	// First, collect all unique controller files and the handlers that belong to them
	controllerFiles := make(map[string]string)      // controllerName -> filePath
	controllerHandlers := make(map[string][]string) // controllerName -> []handlerFuncName
	routeMap := make(map[string]RouteInfo)          // handlerFuncName -> RouteInfo

	// Collect route information
	for httpMethod, paths := range router.Routes {
		if paths == nil {
			continue
		}

		for path, route := range paths {
			if route.Handler == nil {
				continue
			}

			// Get the function name from the handler
			funcName := runtime.FuncForPC(reflect.ValueOf(route.Handler).Pointer()).Name()

			// Extract just the function name (remove package path)
			parts := strings.Split(funcName, ".")

			if len(parts) == 0 {
				continue
			}

			handlerFuncName := parts[len(parts)-1]

			// Skip non-controller functions
			if !strings.Contains(handlerFuncName, "Controller") {
				continue
			}

			// Store route info
			routeMap[handlerFuncName] = RouteInfo{
				HTTPMethod: strings.ToUpper(httpMethod),
				Path:       path,
				Middleware: route.RegisteredMiddleware,
			}

			// Extract controller name and determine file path
			controllerName := extractControllerName(handlerFuncName)

			if controllerName == "" {
				continue
			}

			controllerFile := inferControllerFilePath(controllerName)
			controllerFiles[controllerName] = controllerFile

			// Track which handlers belong to this controller
			if controllerHandlers[controllerName] == nil {
				controllerHandlers[controllerName] = []string{}
			}

			controllerHandlers[controllerName] = append(controllerHandlers[controllerName], handlerFuncName)
		}
	}

	// Now analyze each unique controller file
	for controllerName, controllerFile := range controllerFiles {
		if analyzedFiles[controllerFile] {
			continue
		}

		analyzedFiles[controllerFile] = true

		// Check if the file exists
		if _, err := os.Stat(controllerFile); os.IsNotExist(err) {
			continue // Skip if we can't find the controller file
		}

		// Get the specific handler functions for this controller
		handlerNames := controllerHandlers[controllerName]

		// Analyze the controller file with specific handler names
		controllerAnalysis, err := g.AnalyzeControllerWithHandlers(controllerFile, handlerNames)

		if err != nil {
			continue // Skip if analysis fails
		}

		// Merge the analysis results and apply route information
		for methodName, methodAnalysis := range controllerAnalysis.Methods {
			// Check if we have route information for this method
			if routeInfo, exists := routeMap[methodName]; exists {
				methodAnalysis.HTTPMethod = routeInfo.HTTPMethod
				methodAnalysis.Path = routeInfo.Path
				methodAnalysis.Security = extractSecurityFromMiddleware(routeInfo.Middleware)
			}

			analysis.Methods[methodName] = methodAnalysis
		}
	}

	return analysis, nil
}

// AnalyzeControllerWithHandlers analyzes a controller file for specific handler function names
func (g *Generator) AnalyzeControllerWithHandlers(filePath string, handlerNames []string) (*ControllerAnalysis, error) {
	content, err := os.ReadFile(filePath)

	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", filePath, err)
	}

	// Parse the file
	node, err := parser.ParseFile(g.fileSet, filePath, content, parser.ParseComments)

	if err != nil {
		return nil, fmt.Errorf("failed to parse file %s: %w", filePath, err)
	}

	analysis := &ControllerAnalysis{
		Methods: make(map[string]*MethodAnalysis),
	}

	// Create a set of handler names for fast lookup
	handlerSet := make(map[string]bool)

	for _, name := range handlerNames {
		handlerSet[name] = true
	}

	// First pass: collect type information
	g.collectTypeInfo(node)

	// Second pass: analyze only the specific handler functions
	ast.Inspect(node, func(n ast.Node) bool {
		if fn, ok := n.(*ast.FuncDecl); ok {
			// Check if this function is one of our target handlers
			if handlerSet[fn.Name.Name] {
				methodAnalysis := g.analyzeControllerMethod(fn, content)
				if methodAnalysis != nil {
					analysis.Methods[fn.Name.Name] = methodAnalysis
				}
			}
		}

		return true
	})

	return analysis, nil
}

// RouteInfo holds information about a route
type RouteInfo struct {
	HTTPMethod string
	Path       string
	Middleware []http.Middleware
}

// extractControllerName extracts controller name from handler function name
func extractControllerName(handlerFuncName string) string {
	// Handle naming pattern: "ResourceControllerMethod" (e.g., "UserControllerIndex")
	re1 := regexp.MustCompile(`^(\w+Controller)(\w+)$`)
	matches1 := re1.FindStringSubmatch(handlerFuncName)

	if len(matches1) >= 2 {
		return matches1[1]
	}

	// Fallback: if it ends with "Controller" but doesn't match pattern above,
	// treat the whole thing as a controller name (e.g., "HealthCheckController")
	if strings.HasSuffix(handlerFuncName, "Controller") {
		return handlerFuncName
	}

	// If no pattern matches, return empty string
	return ""
}

// extractTagFromControllerName converts controller name to tag
func extractTagFromControllerName(funcName string) string {
	// Handle naming pattern: "ResourceControllerMethod" (e.g., "UserControllerIndex")
	re1 := regexp.MustCompile(`^(\w+)Controller\w+$`)
	matches1 := re1.FindStringSubmatch(funcName)

	if len(matches1) >= 2 {
		return matches1[1] // e.g., "User" -> "User" (no pluralization)
	}

	// Fallback: if it ends with "Controller" but doesn't match pattern above,
	// treat it as a resource name (e.g., "HealthCheckController" -> "HealthCheck")
	if strings.HasSuffix(funcName, "Controller") {
		resourceName := strings.TrimSuffix(funcName, "Controller")
		if resourceName != "" {
			return resourceName
		}
	}

	return "API"
}

// pluralizeCompoundWord handles pluralization of compound words by pluralizing the last component
func pluralizeCompoundWord(word string) string {
	parts := splitCamelCase(word)
	if len(parts) > 1 {
		// Pluralize the last part
		lastIdx := len(parts) - 1
		parts[lastIdx] = pluralizePart(parts[lastIdx])

		// Reconstruct the compound word
		return strings.Join(parts, "")
	}

	// Single word, use regular pluralization
	return pluralizePart(word)
}

// pluralizePart handles pluralization of a single word part
func pluralizePart(word string) string {
	if word == "" {
		return word
	}

	lower := strings.ToLower(word)

	// Common irregular plurals
	irregulars := map[string]string{
		"child":  "children",
		"foot":   "feet",
		"tooth":  "teeth",
		"mouse":  "mice",
		"goose":  "geese",
		"man":    "men",
		"woman":  "women",
		"person": "people",
	}

	if plural, ok := irregulars[lower]; ok {
		// Preserve original case
		if isUpperCase(word[0:1]) {
			caser := cases.Title(language.English)
			return caser.String(plural)
		}
		return plural
	}

	// Apply regular pluralization rules to the word
	if strings.HasSuffix(lower, "ch") || strings.HasSuffix(lower, "sh") ||
		strings.HasSuffix(lower, "s") || strings.HasSuffix(lower, "x") ||
		strings.HasSuffix(lower, "z") {
		return word + "es"
	}

	if strings.HasSuffix(lower, "y") && len(word) > 1 {
		penultimate := strings.ToLower(word[len(word)-2 : len(word)-1])
		if !strings.Contains("aeiou", penultimate) {
			return word[:len(word)-1] + "ies"
		}
	}

	if strings.HasSuffix(lower, "f") {
		return word[:len(word)-1] + "ves"
	}

	if strings.HasSuffix(lower, "fe") {
		return word[:len(word)-2] + "ves"
	}

	// Default case: just add 's'
	return word + "s"
}

// isUpperCase checks if the string contains an uppercase letter
func isUpperCase(s string) bool {
	return s != strings.ToLower(s)
}

// pluralize converts singular nouns to plural, handling irregular plurals
func pluralize(word string) string {
	// Handle compound words dynamically
	if hasCompoundStructure(word) {
		return pluralizeCompoundWord(word)
	}

	// For simple words, use the same logic as compound parts
	return pluralizePart(word)
}

// singularize converts plural nouns to singular, handling English pluralization rules
func singularize(word string) string {
	if word == "" {
		return word
	}

	original := word
	lowerWord := strings.ToLower(word)

	// Handle compound words by detecting camelCase/PascalCase patterns
	if hasCompoundStructure(original) {
		return singularizeCompoundWord(original)
	}

	// Handle irregular plurals first
	irregularPlurals := map[string]string{
		"children": "child",
		"feet":     "foot",
		"geese":    "goose",
		"men":      "man",
		"mice":     "mouse",
		"people":   "person",
		"teeth":    "tooth",
		"women":    "woman",
		"oxen":     "ox",
	}

	if singular, exists := irregularPlurals[lowerWord]; exists {
		return preserveCase(original, singular)
	}

	// For non-compound words, use the simple singularization
	return singularizeSimpleWord(original)
}

// hasCompoundStructure detects if a word has camelCase/PascalCase compound structure
func hasCompoundStructure(word string) bool {
	// Look for internal capital letters (indicating camelCase/PascalCase)
	capitalCount := 0
	for _, r := range word {
		if r >= 'A' && r <= 'Z' {
			capitalCount++
		}
	}
	// If we have more than one capital letter, it's likely a compound word
	// Also check for common compound patterns
	return capitalCount > 1 || containsCommonCompoundPattern(word)
}

// containsCommonCompoundPattern checks for common compound word patterns
func containsCommonCompoundPattern(word string) bool {
	lowerWord := strings.ToLower(word)

	// Only consider it a compound if it has a meaningful compound structure
	// Not just any word containing these patterns
	compoundIndicators := []struct {
		pattern     string
		minTotalLen int
	}{
		{"database", 10}, // "databases" = 9, so 10+ means real compound like "databasebackup"
		{"access", 8},    // "accesskey" would be 9
		{"health", 8},    // "healthcheck" would be 11
	}

	for _, indicator := range compoundIndicators {
		if strings.Contains(lowerWord, indicator.pattern) && len(lowerWord) >= indicator.minTotalLen {
			return true
		}
	}
	return false
}

// singularizeCompoundWord handles compound words by breaking them into parts
func singularizeCompoundWord(word string) string {
	// Split camelCase/PascalCase into parts
	parts := splitCamelCase(word)
	if len(parts) == 0 {
		return word
	}

	// Singularize the last part (which is typically the plural part)
	lastIndex := len(parts) - 1
	parts[lastIndex] = singularizeSimpleWord(parts[lastIndex])

	// Rejoin the parts
	return strings.Join(parts, "")
}

// splitCamelCase splits a camelCase/PascalCase word into individual words
func splitCamelCase(word string) []string {
	// First try to split by capital letters
	var parts []string
	var currentPart strings.Builder

	for i, r := range word {
		if i > 0 && r >= 'A' && r <= 'Z' {
			// Found a capital letter, start a new part
			if currentPart.Len() > 0 {
				parts = append(parts, currentPart.String())
				currentPart.Reset()
			}
		}
		currentPart.WriteRune(r)
	}

	// Add the last part
	if currentPart.Len() > 0 {
		parts = append(parts, currentPart.String())
	}

	// If we only got one part, try to split by compound patterns
	if len(parts) == 1 {
		parts = splitByCompoundPatterns(word)
	}

	return parts
}

// splitByCompoundPatterns splits words by known compound patterns
func splitByCompoundPatterns(word string) []string {
	lowerWord := strings.ToLower(word)

	// Known compound word patterns (prefix + suffix)
	patterns := []struct {
		prefix       string
		minSuffixLen int
	}{
		{"database", 3}, // Need at least 3 characters after "database"
		{"access", 3},   // Need at least 3 characters after "access"
		{"health", 3},   // Need at least 3 characters after "health"
	}

	for _, pattern := range patterns {
		if strings.HasPrefix(lowerWord, pattern.prefix) {
			remaining := word[len(pattern.prefix):]
			if len(remaining) >= pattern.minSuffixLen {
				// Preserve original case for both parts
				prefix := word[:len(pattern.prefix)]
				return []string{prefix, remaining}
			}
		}
	}

	// If no patterns match, return the original word as a single part
	return []string{word}
}

// singularizeSimpleWord singularizes a simple (non-compound) word
func singularizeSimpleWord(word string) string {
	if word == "" {
		return word
	}

	original := word
	lowerWord := strings.ToLower(word)

	// Handle irregular plurals
	irregularPlurals := map[string]string{
		"children": "child",
		"feet":     "foot",
		"geese":    "goose",
		"men":      "man",
		"mice":     "mouse",
		"people":   "person",
		"teeth":    "tooth",
		"women":    "woman",
		"oxen":     "ox",
	}

	if singular, exists := irregularPlurals[lowerWord]; exists {
		return preserveCase(original, singular)
	}

	// Rule 1: Words ending in 'ies' -> change to 'y' (but not if preceded by vowel)
	if strings.HasSuffix(lowerWord, "ies") && len(lowerWord) > 3 {
		beforeIes := lowerWord[len(lowerWord)-4]
		if !isVowel(beforeIes) {
			result := lowerWord[:len(lowerWord)-3] + "y"
			return preserveCase(original, result)
		}
	}

	// Rule 2: Words ending in 'ves' -> change to 'f' or 'fe'
	if strings.HasSuffix(lowerWord, "ves") && len(lowerWord) > 3 {
		stem := lowerWord[:len(lowerWord)-3]
		result := stem + "f"
		return preserveCase(original, result)
	}

	// Rule 3: Words ending in 'es' after s, x, z, ch, sh
	if strings.HasSuffix(lowerWord, "es") && len(lowerWord) > 2 {
		beforeEs := lowerWord[len(lowerWord)-3:]
		if strings.HasSuffix(beforeEs, "ses") || strings.HasSuffix(beforeEs, "xes") ||
			strings.HasSuffix(beforeEs, "zes") || strings.HasSuffix(beforeEs, "ches") ||
			strings.HasSuffix(beforeEs, "shes") {
			result := lowerWord[:len(lowerWord)-2]
			return preserveCase(original, result)
		}
	}

	// Rule 4: Words ending in 'oes' -> usually change to 'o'
	if strings.HasSuffix(lowerWord, "oes") && len(lowerWord) > 3 {
		result := lowerWord[:len(lowerWord)-2]
		return preserveCase(original, result)
	}

	// Rule 5: Words ending in 'i' -> change to 'us' (Latin plurals)
	if strings.HasSuffix(lowerWord, "i") && len(lowerWord) > 1 {
		if strings.HasSuffix(lowerWord, "alumni") {
			result := lowerWord[:len(lowerWord)-1] + "us"
			return preserveCase(original, result)
		}
	}

	// Rule 6: Words ending in 'a' -> change to 'um' (Latin plurals)
	if strings.HasSuffix(lowerWord, "a") && len(lowerWord) > 1 {
		if strings.HasSuffix(lowerWord, "data") || strings.HasSuffix(lowerWord, "criteria") {
			result := lowerWord[:len(lowerWord)-1] + "um"
			return preserveCase(original, result)
		}
	}

	// Rule 7: Default rule - words ending in 's' -> remove 's'
	if strings.HasSuffix(lowerWord, "s") && len(lowerWord) > 1 {
		// Don't singularize words that naturally end in 's'
		naturalSWords := []string{"bass", "pass", "class", "mass", "glass", "grass", "stress", "process", "success", "access", "address", "business", "express", "progress", "congress"}
		for _, natural := range naturalSWords {
			if lowerWord == natural {
				return preserveCase(original, lowerWord)
			}
		}

		// Try removing just 's' first
		candidate := lowerWord[:len(lowerWord)-1]

		// Check for common patterns where removing 's' creates invalid words
		invalidCandidates := []string{"databas", "addres", "addresse", "busines", "proces", "processe", "succes", "acces", "expres", "progres", "congres", "branche", "classe"}
		for _, invalid := range invalidCandidates {
			if candidate == invalid {
				// These are likely correct patterns, handle them specifically
				corrections := map[string]string{
					"databas":  "database",
					"addres":   "address",
					"addresse": "address",
					"busines":  "business",
					"proces":   "process",
					"processe": "process",
					"succes":   "success",
					"acces":    "access",
					"expres":   "express",
					"progres":  "progress",
					"congres":  "congress",
					"branche":  "branch",
					"classe":   "class",
				}
				if corrected, exists := corrections[candidate]; exists {
					candidate = corrected
				}
				break
			}
		}

		result := candidate
		return preserveCase(original, result)
	}

	// If no rules apply, return the original word
	return original
}

// Helper functions for the singularize function
func isVowel(c byte) bool {
	vowels := "aeiou"
	return strings.ContainsRune(vowels, rune(c))
}

func isAllUpper(s string) bool {
	for _, r := range s {
		if r >= 'a' && r <= 'z' {
			return false
		}
	}
	return true
}

func isCapitalized(s string) bool {
	if len(s) == 0 {
		return false
	}
	first := rune(s[0])
	return first >= 'A' && first <= 'Z'
}

func preserveCase(original, result string) string {
	if isAllUpper(original) {
		return strings.ToUpper(result)
	} else if isCapitalized(original) {
		return capitalizeFirst(result)
	}
	return result
}

// extractPathParameters extracts parameter names from path patterns like "/users/{username}"
func extractPathParameters(path string) []string {
	re := regexp.MustCompile(`\{([^}]+)\}`)
	matches := re.FindAllStringSubmatch(path, -1)
	var params []string

	for _, match := range matches {
		if len(match) >= 2 {
			params = append(params, match[1])
		}
	}

	return params
}

// extractResourceTypeFromMethod extracts resource type from method name like "UserControllerIndex" -> "User"
func extractResourceTypeFromMethod(methodName string) string {
	re := regexp.MustCompile(`^(\w+)Controller\w+$`)
	matches := re.FindStringSubmatch(methodName)

	if len(matches) >= 2 {
		return matches[1]
	}

	return "Resource"
}

// inferControllerFilePath infers controller file path from controller name
func inferControllerFilePath(controllerName string) string {
	// Remove "Controller" suffix
	baseName := strings.TrimSuffix(controllerName, "Controller")

	// Convert camelCase to snake_case
	// e.g., "AccessKey" -> "access_key", "Database" -> "database", "User" -> "user"
	snakeName := camelToSnake(baseName)

	// Try different path variations to accommodate different working directories
	possiblePaths := []string{
		fmt.Sprintf("pkg/http/%s_controller.go", snakeName),       // From project root
		fmt.Sprintf("../http/%s_controller.go", snakeName),        // From pkg/openapi
		fmt.Sprintf("../../pkg/http/%s_controller.go", snakeName), // From cmd subdirectory
	}

	// Return the first path that exists
	for _, path := range possiblePaths {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}

	// Fallback to the original path if none exist
	return fmt.Sprintf("pkg/http/%s_controller.go", snakeName)
}

// camelToSnake converts CamelCase to snake_case
func camelToSnake(s string) string {
	var result []rune

	for i, r := range s {
		if i > 0 && isUpper(r) {
			result = append(result, '_')
		}

		result = append(result, toLower(r))
	}

	return string(result)
}

func isUpper(r rune) bool {
	return r >= 'A' && r <= 'Z'
}

func toLower(r rune) rune {
	if r >= 'A' && r <= 'Z' {
		return r - 'A' + 'a'
	}

	return r
}

// capitalizeFirst capitalizes the first letter of a string
func capitalizeFirst(s string) string {
	if s == "" {
		return s
	}

	runes := []rune(s)

	if runes[0] >= 'a' && runes[0] <= 'z' {
		runes[0] = runes[0] - 'a' + 'A'
	}

	return string(runes)
}

// convertResourceToDisplayName converts compound resource names to proper display format
// e.g., "accesskey" -> "access key", "databasebackup" -> "database backup"
func convertResourceToDisplayName(resourceName string) string {
	// For compound words in PascalCase or camelCase, split and lowercase
	if hasCompoundStructure(resourceName) {
		parts := splitCamelCase(resourceName)
		var displayParts []string
		for _, part := range parts {
			displayParts = append(displayParts, strings.ToLower(part))
		}
		return strings.Join(displayParts, " ")
	}

	// For simple resources, return lowercase
	return strings.ToLower(resourceName)
}

// getIndefiniteArticle returns "a" or "an" based on the first sound
func getIndefiniteArticle(word string) string {
	if len(word) == 0 {
		return "a"
	}

	// Handle special cases first (words that start with vowels but have consonant sounds)
	lowerWord := strings.ToLower(word)

	specialCases := map[string]string{
		"user":    "a",
		"unique":  "a",
		"uniform": "a",
		"unit":    "a",
		"usage":   "a",
	}

	if article, exists := specialCases[lowerWord]; exists {
		return article
	}

	firstChar := strings.ToLower(string(word[0]))
	vowels := []string{"a", "e", "i", "o", "u"}

	if slices.Contains(vowels, firstChar) {
		return "an"
	}

	return "a"
}

// extractSecurityFromMiddleware analyzes middleware to determine security requirements
func extractSecurityFromMiddleware(middleware []http.Middleware) []string {
	var security []string

	for _, mw := range middleware {
		if mw == nil {
			continue
		}

		// Get middleware function name using reflection
		funcName := runtime.FuncForPC(reflect.ValueOf(mw).Pointer()).Name()
		parts := strings.Split(funcName, ".")

		if len(parts) == 0 {
			continue
		}

		middlewareName := parts[len(parts)-1]

		// Map middleware names to security schemes
		switch middlewareName {
		case "Authentication":
			// Authentication middleware allows all three auth methods
			security = append(security, "AccessKeyAuth")
			security = append(security, "BasicAuth")
			security = append(security, "TokenAuth")
		case "Internal":
			security = append(security, "InternalAuth")
		}
	}

	return security
}

// AnalyzeController performs comprehensive analysis of a controller file
func (g *Generator) AnalyzeController(filePath string, controllerName string) (*ControllerAnalysis, error) {
	content, err := os.ReadFile(filePath)

	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", filePath, err)
	}

	// Parse the file
	node, err := parser.ParseFile(g.fileSet, filePath, content, parser.ParseComments)

	if err != nil {
		return nil, fmt.Errorf("failed to parse file %s: %w", filePath, err)
	}

	analysis := &ControllerAnalysis{
		Methods: make(map[string]*MethodAnalysis),
	}

	// First pass: collect type information
	g.collectTypeInfo(node)

	// Second pass: analyze controller methods
	ast.Inspect(node, func(n ast.Node) bool {
		if fn, ok := n.(*ast.FuncDecl); ok {
			if strings.Contains(fn.Name.Name, controllerName) {
				methodAnalysis := g.analyzeControllerMethod(fn, content)

				if methodAnalysis != nil {
					analysis.Methods[fn.Name.Name] = methodAnalysis
				}
			}
		}

		return true
	})

	return analysis, nil
}

// collectTypeInfo collects information about types defined in the file
func (g *Generator) collectTypeInfo(node *ast.File) {
	// First, collect imports for cross-package type resolution
	g.collectImports(node)

	// Then collect types from current file
	ast.Inspect(node, func(n ast.Node) bool {
		switch decl := n.(type) {
		case *ast.GenDecl:
			if decl.Tok == token.TYPE {
				for _, spec := range decl.Specs {
					if typeSpec, ok := spec.(*ast.TypeSpec); ok {
						g.analyzeTypeSpec(typeSpec, decl.Doc)
					}
				}
			}
		}

		return true
	})
}

// collectImports collects import statements and builds import mapping
func (g *Generator) collectImports(node *ast.File) {
	for _, imp := range node.Imports {
		importPath := strings.Trim(imp.Path.Value, `"`)

		// Determine package alias/name
		var packageName string

		if imp.Name != nil {
			packageName = imp.Name.Name
		} else {
			// Extract package name from import path
			parts := strings.Split(importPath, "/")
			packageName = parts[len(parts)-1]
		}

		g.importCache[packageName] = importPath
	}
}

// analyzeTypeSpec analyzes a type specification
func (g *Generator) analyzeTypeSpec(typeSpec *ast.TypeSpec, docGroup *ast.CommentGroup) {
	typeName := typeSpec.Name.Name

	typeInfo := &TypeInfo{
		Name:   typeName,
		Fields: make(map[string]*FieldInfo),
	}

	// Extract description from comments
	if docGroup != nil {
		typeInfo.Description = extractCommentText(docGroup)
	}

	// Analyze struct type
	if structType, ok := typeSpec.Type.(*ast.StructType); ok {
		typeInfo.Type = "object"

		for _, field := range structType.Fields.List {
			fieldInfo := g.analyzeStructField(field)

			if fieldInfo != nil {
				typeInfo.Fields[fieldInfo.Name] = fieldInfo
			}
		}
	}

	g.typeInfo[typeName] = typeInfo
}

// analyzeStructField analyzes a struct field
func (g *Generator) analyzeStructField(field *ast.Field) *FieldInfo {
	if len(field.Names) == 0 {
		return nil // Anonymous field
	}

	fieldName := field.Names[0].Name

	fieldInfo := &FieldInfo{
		Name:       fieldName,
		Validation: make(map[string]string),
	}

	// Extract type
	fieldInfo.Type = g.extractTypeString(field.Type)

	// Extract JSON tag and validation
	if field.Tag != nil {
		tagValue := strings.Trim(field.Tag.Value, "`")
		g.parseStructTags(tagValue, fieldInfo)
	}

	// Extract description from comments
	if field.Doc != nil {
		fieldInfo.Description = extractCommentText(field.Doc)
	} else if field.Comment != nil {
		fieldInfo.Description = extractCommentText(field.Comment)
	}

	return fieldInfo
}

// parseStructTags parses struct tags to extract JSON name and validation rules
func (g *Generator) parseStructTags(tagValue string, fieldInfo *FieldInfo) {
	// Parse JSON tag
	if jsonMatch := regexp.MustCompile(`json:"([^"]+)"`).FindStringSubmatch(tagValue); len(jsonMatch) > 1 {
		jsonParts := strings.Split(jsonMatch[1], ",")
		fieldInfo.JSONName = jsonParts[0]

		// Check for omitempty
		for _, part := range jsonParts[1:] {
			if part == "omitempty" {
				fieldInfo.Required = false
			}
		}
	}

	// Parse validation tag
	if validateMatch := regexp.MustCompile(`validate:"([^"]+)"`).FindStringSubmatch(tagValue); len(validateMatch) > 1 {
		validationRules := strings.SplitSeq(validateMatch[1], ",")

		for rule := range validationRules {
			if rule == "required" {
				fieldInfo.Required = true
			} else if strings.Contains(rule, "=") {
				parts := strings.SplitN(rule, "=", 2)
				fieldInfo.Validation[parts[0]] = parts[1]
			} else {
				fieldInfo.Validation[rule] = "true"
			}
		}
	}
}

// extractTypeString extracts type information as string
func (g *Generator) extractTypeString(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.ArrayType:
		return "array[" + g.extractTypeString(t.Elt) + "]"
	case *ast.StarExpr:
		return "*" + g.extractTypeString(t.X)
	case *ast.SelectorExpr:
		return g.extractTypeString(t.X) + "." + t.Sel.Name
	default:
		return "unknown"
	}
}

// analyzeControllerMethod analyzes a controller method
func (g *Generator) analyzeControllerMethod(fn *ast.FuncDecl, source []byte) *MethodAnalysis {
	methodName := fn.Name.Name

	analysis := &MethodAnalysis{
		Name:      methodName,
		Responses: make(map[string]*ResponseInfo),
		Tags:      []string{extractTagFromControllerName(fn.Name.Name)},
	}

	// Extract description from comments
	if fn.Doc != nil {
		analysis.Description = extractCommentText(fn.Doc)
	}

	// HTTP method and path will be set from actual route data when available
	// This is just fallback logic for standalone analysis
	analysis.HTTPMethod, analysis.Path = "GET", "/api"

	// Analyze function parameters
	g.analyzeMethodParameters(fn, analysis)

	// First, build a variable type map for this method
	variableTypes := g.buildVariableTypeMap(fn)

	// Analyze function body for responses (with variable context)
	g.analyzeMethodResponsesWithContext(fn, analysis, variableTypes)

	// Analyze authorization calls for security
	g.analyzeMethodSecurity(fn, analysis)

	return analysis
}

// buildVariableTypeMap builds a map of variable names to their types within a function
func (g *Generator) buildVariableTypeMap(fn *ast.FuncDecl) map[string]string {
	variableTypes := make(map[string]string)

	if fn.Body == nil {
		return variableTypes
	}

	// Look for variable assignments and declarations
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.AssignStmt:
			// Handle := assignments like: userResponse := &auth.UserResponse{...}
			if node.Tok == token.DEFINE || node.Tok == token.ASSIGN {
				if len(node.Lhs) == 1 && len(node.Rhs) == 1 {
					if ident, ok := node.Lhs[0].(*ast.Ident); ok {
						varName := ident.Name
						// Extract type from RHS
						if typeName := g.extractTypeFromExpression(node.Rhs[0]); typeName != "" {
							variableTypes[varName] = typeName
						}
					}
				}
			}
		case *ast.GenDecl:
			// Handle var declarations like: var userResponse *auth.UserResponse
			if node.Tok == token.VAR {
				for _, spec := range node.Specs {
					if valueSpec, ok := spec.(*ast.ValueSpec); ok {
						for i, name := range valueSpec.Names {
							varName := name.Name

							if valueSpec.Type != nil {
								typeName := g.extractTypeString(valueSpec.Type)
								variableTypes[varName] = typeName
							} else if i < len(valueSpec.Values) {
								// Infer type from value
								if typeName := g.extractTypeFromExpression(valueSpec.Values[i]); typeName != "" {
									variableTypes[varName] = typeName
								}
							}
						}
					}
				}
			}
		}

		return true
	})

	return variableTypes
}

// extractTypeFromExpression extracts the type from an expression (for variable assignments)
func (g *Generator) extractTypeFromExpression(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.UnaryExpr:
		// Handle &Type{...} patterns
		if e.Op == token.AND {
			if comp, ok := e.X.(*ast.CompositeLit); ok {
				return g.extractTypeString(comp.Type)
			}
		}
	case *ast.CompositeLit:
		// Handle Type{...} patterns
		return g.extractTypeString(e.Type)
	case *ast.CallExpr:
		// Handle function calls that return typed values
		if sel, ok := e.Fun.(*ast.SelectorExpr); ok {
			// Could be a constructor call, but this is complex to analyze
			_ = sel
		}
	}

	return ""
}

// analyzeMethodResponsesWithContext analyzes method responses with variable type context
func (g *Generator) analyzeMethodResponsesWithContext(fn *ast.FuncDecl, analysis *MethodAnalysis, variableTypes map[string]string) {
	if fn.Body == nil {
		return
	}

	// Look for Response{StatusCode: xxx} patterns and helper functions
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.CallExpr:
			g.analyzeResponseCallWithContext(node, analysis, variableTypes)
		case *ast.CompositeLit:
			g.analyzeResponseLiteral(node, analysis)
		}

		return true
	})

	// Add default responses if none found
	if len(analysis.Responses) == 0 {
		g.addDefaultResponses(analysis)
	}
}

// analyzeMethodParameters analyzes method parameters
func (g *Generator) analyzeMethodParameters(fn *ast.FuncDecl, analysis *MethodAnalysis) {
	if fn.Type.Params == nil {
		return
	}

	for _, param := range fn.Type.Params.List {
		if len(param.Names) > 0 {
			paramName := param.Names[0].Name
			paramType := g.extractTypeString(param.Type)

			// Skip context and request parameters
			if paramName == "ctx" || paramName == "request" {
				continue
			}

			paramInfo := &ParameterInfo{
				Name: paramName,
				Type: paramType,
			}

			analysis.Parameters = append(analysis.Parameters, paramInfo)
		}
	}

	// Add path parameters based on the path
	pathParams := extractPathParameters(analysis.Path)

	for _, param := range pathParams {
		analysis.Parameters = append(analysis.Parameters, &ParameterInfo{
			Name:        param,
			In:          "path",
			Type:        "string",
			Required:    true,
			Description: fmt.Sprintf("The %s parameter", param),
			Example:     "example_value",
		})
	}
}

// analyzeResponseCallWithContext analyzes response function calls with variable context
func (g *Generator) analyzeResponseCallWithContext(call *ast.CallExpr, analysis *MethodAnalysis, variableTypes map[string]string) {
	if ident, ok := call.Fun.(*ast.Ident); ok {
		switch ident.Name {
		case "ForbiddenResponse":
			analysis.Responses["403"] = &ResponseInfo{
				StatusCode:  403,
				Description: "Forbidden - insufficient permissions",
				Type:        "error",
			}
		case "NotFoundResponse":
			analysis.Responses["404"] = &ResponseInfo{
				StatusCode:  404,
				Description: "Resource not found",
				Type:        "error",
			}
		case "BadRequestResponse":
			analysis.Responses["400"] = &ResponseInfo{
				StatusCode:  400,
				Description: "Bad request - invalid input",
				Type:        "error",
			}
		case "ValidationErrorResponse":
			analysis.Responses["422"] = &ResponseInfo{
				StatusCode:  422,
				Description: "Validation error",
				Type:        "validation_error",
			}
		case "ServerErrorResponse":
			analysis.Responses["500"] = &ResponseInfo{
				StatusCode:  500,
				Description: "Internal server error",
				Type:        "error",
			}
		case "SuccessResponse":
			// Extract the data type from SuccessResponse arguments
			statusCode := 200
			var dataSchema *Schema
			successMessage := "Successful operation" // Default message

			// Extract success message from first argument (message parameter)
			if len(call.Args) >= 1 {
				if basicLit, ok := call.Args[0].(*ast.BasicLit); ok && basicLit.Kind == token.STRING {
					// Remove quotes from string literal and use as success message
					successMessage = strings.Trim(basicLit.Value, `"`)
				}
			}

			// Check for status code in the third argument
			if len(call.Args) >= 3 {
				if basicLit, ok := call.Args[2].(*ast.BasicLit); ok {
					if code, err := strconv.Atoi(basicLit.Value); err == nil {
						statusCode = code
					}
				}
			}

			// Extract schema from second argument (data parameter)
			if len(call.Args) >= 2 {
				// Check if the data argument is nil
				if ident, ok := call.Args[1].(*ast.Ident); ok && ident.Name == "nil" {
					// Explicitly set dataSchema to nil for nil data
					dataSchema = nil
				} else {
					dataSchema = g.extractSchemaFromExpressionWithContext(call.Args[1], variableTypes)

					// Debug: check what we extracted
					if dataSchema != nil && dataSchema.Type == "object" && len(dataSchema.Properties) == 0 {
						// Try to detect &pkg.ResponseType pattern
						if unary, ok := call.Args[1].(*ast.UnaryExpr); ok && unary.Op == token.AND {
							if comp, ok := unary.X.(*ast.CompositeLit); ok {
								if sel, ok := comp.Type.(*ast.SelectorExpr); ok {
									if ident, ok := sel.X.(*ast.Ident); ok {
										typeName := ident.Name + "." + sel.Sel.Name
										dataSchema = g.createSchemaForKnownType(typeName)
									}
								}
							}
						}
					}
				}
			}

			statusCodeStr := fmt.Sprintf("%d", statusCode)

			analysis.Responses[statusCodeStr] = &ResponseInfo{
				StatusCode:  statusCode,
				Description: "Successful operation",
				Type:        "success",
				Schema:      dataSchema,
				Message:     successMessage, // Store the actual success message
			}
		}
	}
}

// extractSchemaFromExpression extracts OpenAPI schema from an AST expression
func (g *Generator) extractSchemaFromExpression(expr ast.Expr) *Schema {
	return g.extractSchemaFromExpressionWithContext(expr, nil)
}

// extractSchemaFromExpressionWithContext extracts OpenAPI schema from an AST expression with variable context
func (g *Generator) extractSchemaFromExpressionWithContext(expr ast.Expr, variableTypes map[string]string) *Schema {
	switch e := expr.(type) {
	case *ast.UnaryExpr:
		// Handle &Type{...} patterns
		if e.Op == token.AND {
			return g.extractSchemaFromExpressionWithContext(e.X, variableTypes)
		}
	case *ast.CompositeLit:
		// Handle composite literals (struct literals)
		if sel, ok := e.Type.(*ast.SelectorExpr); ok {
			// Package.Type reference
			if ident, ok := sel.X.(*ast.Ident); ok {
				typeName := ident.Name + "." + sel.Sel.Name
				return g.createSchemaForKnownType(typeName)
			}
		} else if ident, ok := e.Type.(*ast.Ident); ok {
			// Local type reference
			return g.createSchemaForKnownType(ident.Name)
		}
	case *ast.Ident:
		// Check if we have variable type information
		if variableTypes != nil {
			if varType, exists := variableTypes[e.Name]; exists {
				return g.createSchemaForKnownType(varType)
			}
		}

		// Handle variable references - look for common response patterns
		if strings.HasSuffix(e.Name, "Response") || strings.HasSuffix(e.Name, "Responses") {
			if strings.HasSuffix(e.Name, "Responses") {
				// Array of responses
				return &Schema{
					Type:        "array",
					Description: "Array of response objects",
					Items:       g.createSchemaForKnownType(strings.TrimSuffix(e.Name, "s")),
				}
			}

			return g.createSchemaForKnownType(e.Name)
		}

		return g.createSchemaForKnownType(e.Name)
	case *ast.SelectorExpr:
		// Handle pkg.Type references
		if ident, ok := e.X.(*ast.Ident); ok {
			typeName := ident.Name + "." + e.Sel.Name

			return g.createSchemaForKnownType(typeName)
		}
	}

	// Default to generic object
	return &Schema{Type: "object"}
}

// createSchemaForKnownType creates a schema for known types
func (g *Generator) createSchemaForKnownType(typeName string) *Schema {
	// First, check if we have type information from our analysis
	if typeInfo, exists := g.typeInfo[typeName]; exists {
		return g.convertTypeInfoToSchema(typeInfo)
	}

	// If it's a qualified type (package.Type), try to discover it dynamically
	if strings.Contains(typeName, ".") {
		if schema := g.discoverExternalType(typeName); schema != nil {
			return schema
		}
	}

	// Extract the simple type name for lookup
	simpleName := typeName

	if strings.Contains(typeName, ".") {
		parts := strings.Split(typeName, ".")
		simpleName = parts[len(parts)-1]
	}

	// Try to find by simple name
	if typeInfo, exists := g.typeInfo[simpleName]; exists {
		return g.convertTypeInfoToSchema(typeInfo)
	}

	// Look for common response patterns and create generic schemas
	if strings.HasSuffix(simpleName, "Response") {
		return &Schema{
			Type: "object",
			Properties: map[string]*Schema{
				"id": {
					Type:        "string",
					Description: "Resource identifier",
					Example:     "example_id",
				},
				"created_at": {
					Type:        "string",
					Format:      "date-time",
					Description: "Creation timestamp",
					Example:     "2023-09-20T14:30:00Z",
				},
				"updated_at": {
					Type:        "string",
					Format:      "date-time",
					Description: "Last update timestamp",
					Example:     "2023-09-20T14:30:00Z",
				},
			},
			Required: []string{"id", "created_at", "updated_at"},
		}
	}

	// Default to generic object
	return &Schema{Type: "object"}
}

// discoverExternalType dynamically discovers and analyzes external types
func (g *Generator) discoverExternalType(qualifiedTypeName string) *Schema {
	parts := strings.Split(qualifiedTypeName, ".")

	if len(parts) != 2 {
		return nil
	}

	packageName := parts[0]
	typeName := parts[1]

	// Check if we have import information for this package
	importPath, exists := g.importCache[packageName]

	if !exists {
		return nil
	}

	// Try to resolve the actual file path for the package
	packagePath := g.resolvePackagePath(importPath)

	if packagePath == "" {
		return nil
	}

	// Parse and analyze the external package
	if files := g.parseExternalPackage(packagePath); files != nil {
		// Look for the type in the parsed package
		for _, file := range files {
			ast.Inspect(file, func(n ast.Node) bool {
				if genDecl, ok := n.(*ast.GenDecl); ok && genDecl.Tok == token.TYPE {
					for _, spec := range genDecl.Specs {
						if typeSpec, ok := spec.(*ast.TypeSpec); ok {
							if typeSpec.Name.Name == typeName {
								// Found the type! Analyze it
								g.analyzeTypeSpecWithPrefix(typeSpec, genDecl.Doc, packageName)

								// Return immediately after finding and analyzing
								return false // Stop searching
							}
						}
					}
				}

				return true
			})
		}

		// If we found and analyzed the type, return its schema
		fullTypeName := packageName + "." + typeName

		if typeInfo, exists := g.typeInfo[fullTypeName]; exists {
			return g.convertTypeInfoToSchema(typeInfo)
		}
	}

	return nil
}

// resolvePackagePath resolves an import path to actual file system path
func (g *Generator) resolvePackagePath(importPath string) string {
	// Handle relative imports within the same project
	if strings.HasPrefix(importPath, "github.com/litebase/litebase/") {
		// Remove the module prefix and construct relative path
		relativePath := strings.TrimPrefix(importPath, "github.com/litebase/litebase/")

		// Try different base paths to find the package
		possiblePaths := []string{
			relativePath,            // From project root
			"../" + relativePath,    // From pkg/openapi
			"../../" + relativePath, // From cmd subdirectory
		}

		for _, path := range possiblePaths {
			if _, err := os.Stat(path); err == nil {
				return path
			}
		}
	}

	return ""
}

// parseExternalPackage parses an external package and caches the result
func (g *Generator) parseExternalPackage(packagePath string) []*ast.File {
	// Check cache first
	if files, exists := g.packageCache[packagePath]; exists {
		return files
	}

	// Parse the package directory
	pkgs, err := parser.ParseDir(g.fileSet, packagePath, nil, parser.ParseComments)

	if err != nil {
		return nil
	}

	// Usually there's only one package per directory
	for _, pkg := range pkgs {
		// Extract files from the package
		var files []*ast.File
		for _, file := range pkg.Files {
			files = append(files, file)
		}
		g.packageCache[packagePath] = files

		return files
	}

	return nil
}

// analyzeTypeSpecWithPrefix analyzes a type spec with a package prefix
func (g *Generator) analyzeTypeSpecWithPrefix(typeSpec *ast.TypeSpec, docGroup *ast.CommentGroup, packagePrefix string) {
	typeName := packagePrefix + "." + typeSpec.Name.Name

	typeInfo := &TypeInfo{
		Name:   typeName,
		Fields: make(map[string]*FieldInfo),
	}

	// Extract description from comments
	if docGroup != nil {
		typeInfo.Description = extractCommentText(docGroup)
	}

	// Analyze struct type
	if structType, ok := typeSpec.Type.(*ast.StructType); ok {
		typeInfo.Type = "object"
		for _, field := range structType.Fields.List {
			fieldInfo := g.analyzeStructField(field)
			if fieldInfo != nil {
				typeInfo.Fields[fieldInfo.Name] = fieldInfo
			}
		}
	}

	g.typeInfo[typeName] = typeInfo
}

// analyzeResponseLiteral analyzes Response{} literals
func (g *Generator) analyzeResponseLiteral(lit *ast.CompositeLit, analysis *MethodAnalysis) {
	if ident, ok := lit.Type.(*ast.Ident); ok && ident.Name == "Response" {
		statusCode := 200
		var bodySchema *Schema

		for _, elt := range lit.Elts {
			if kv, ok := elt.(*ast.KeyValueExpr); ok {
				if key, ok := kv.Key.(*ast.Ident); ok {
					switch key.Name {
					case "StatusCode":
						if basicLit, ok := kv.Value.(*ast.BasicLit); ok {
							if code, err := strconv.Atoi(basicLit.Value); err == nil {
								statusCode = code
							}
						}
					case "Body":
						// Extract schema from the Body field
						bodySchema = g.extractSchemaFromResponseBody(kv.Value, analysis)
					}
				}
			}
		}

		statusCodeStr := fmt.Sprintf("%d", statusCode)

		if _, exists := analysis.Responses[statusCodeStr]; !exists {
			analysis.Responses[statusCodeStr] = &ResponseInfo{
				StatusCode:  statusCode,
				Description: getDefaultResponseDescription(statusCode),
				Type:        "custom",
				Schema:      bodySchema,
			}
		}
	}
}

// extractSchemaFromResponseBody extracts schema from response body expressions
func (g *Generator) extractSchemaFromResponseBody(expr ast.Expr, analysis *MethodAnalysis) *Schema {
	switch e := expr.(type) {
	case *ast.CompositeLit:
		// Handle map[string]any{...} responses
		if _, ok := e.Type.(*ast.MapType); ok {
			schema := &Schema{
				Type:       "object",
				Properties: make(map[string]*Schema),
			}

			// Extract properties from the map literal
			for _, elt := range e.Elts {
				if kv, ok := elt.(*ast.KeyValueExpr); ok {
					if keyLit, ok := kv.Key.(*ast.BasicLit); ok && keyLit.Kind == token.STRING {
						propName := strings.Trim(keyLit.Value, "\"")
						propSchema := g.extractSchemaFromExpression(kv.Value)

						// Special handling for Index methods with "data" array
						if propName == "data" && strings.HasSuffix(analysis.Name, "Index") {
							// For Index methods, assume data is an array of the main resource type
							resourceType := extractResourceTypeFromMethod(analysis.Name)
							propSchema = &Schema{
								Type:        "array",
								Description: "Array of resource objects",
								Items:       g.createSchemaForKnownType(resourceType + "Response"),
							}
						}

						schema.Properties[propName] = propSchema
					}
				}
			}

			return schema
		}
	}

	return g.extractSchemaFromExpression(expr)
}

// analyzeMethodSecurity analyzes authorization calls to determine security requirements
func (g *Generator) analyzeMethodSecurity(fn *ast.FuncDecl, analysis *MethodAnalysis) {
	if fn.Body == nil {
		return
	}

	// Look for request.Authorize calls
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
				if ident, ok := sel.X.(*ast.Ident); ok && ident.Name == "request" && sel.Sel.Name == "Authorize" {
					// When authorization is required, allow all three auth methods
					analysis.Security = append(analysis.Security, "AccessKeyAuth")
					analysis.Security = append(analysis.Security, "BasicAuth")
					analysis.Security = append(analysis.Security, "TokenAuth")
				}
			}
		}
		return true
	})
}

// addDefaultResponses adds default responses based on HTTP method
func (g *Generator) addDefaultResponses(analysis *MethodAnalysis) {
	switch analysis.HTTPMethod {
	case "GET":
		analysis.Responses["200"] = &ResponseInfo{
			StatusCode:  200,
			Description: "Successful operation",
			Type:        "success",
		}
	case "POST":
		analysis.Responses["201"] = &ResponseInfo{
			StatusCode:  201,
			Description: "Resource created successfully",
			Type:        "success",
		}
	case "PUT":
		analysis.Responses["200"] = &ResponseInfo{
			StatusCode:  200,
			Description: "Resource updated successfully",
			Type:        "success",
		}
	case "DELETE":
		analysis.Responses["204"] = &ResponseInfo{
			StatusCode:  204,
			Description: "Resource deleted successfully",
			Type:        "success",
		}
	}
}

// extractCommentText extracts clean text from comment groups
func extractCommentText(commentGroup *ast.CommentGroup) string {
	if commentGroup == nil {
		return ""
	}

	var parts []string

	for _, comment := range commentGroup.List {
		text := strings.TrimPrefix(comment.Text, "//")
		text = strings.TrimPrefix(text, "/*")
		text = strings.TrimSuffix(text, "*/")
		text = strings.TrimSpace(text)

		if text != "" {
			parts = append(parts, text)
		}
	}

	return strings.Join(parts, " ")
}

// getDefaultResponseDescription returns a default description for a status code
func getDefaultResponseDescription(statusCode int) string {
	switch statusCode {
	case 200:
		return "Successful operation"
	case 201:
		return "Resource created successfully"
	case 204:
		return "No content"
	case 400:
		return "Bad request"
	case 401:
		return "Unauthorized"
	case 403:
		return "Forbidden"
	case 404:
		return "Resource not found"
	case 422:
		return "Validation error"
	case 500:
		return "Internal server error"
	default:
		return fmt.Sprintf("Response with status code %d", statusCode)
	}
}

// GenerateOpenAPIFromAnalysis converts controller analysis to OpenAPI operations
func (g *Generator) GenerateOpenAPIFromAnalysis(analysis *ControllerAnalysis) map[string]map[string]*Operation {
	paths := make(map[string]map[string]*Operation)

	for _, methodAnalysis := range analysis.Methods {
		path := methodAnalysis.Path
		httpMethod := strings.ToLower(methodAnalysis.HTTPMethod)

		if paths[path] == nil {
			paths[path] = make(map[string]*Operation)
		}

		operation := &Operation{
			Summary:     generateOperationSummary(methodAnalysis),
			Description: methodAnalysis.Description,
			OperationID: generateOperationID(methodAnalysis),
			Tags:        methodAnalysis.Tags,
			Parameters:  convertParameters(methodAnalysis.Parameters),
			Responses:   convertResponses(methodAnalysis.Responses),
		}

		// Add security if required
		if len(methodAnalysis.Security) > 0 {
			for _, security := range methodAnalysis.Security {
				operation.Security = append(operation.Security, SecurityRequirement{
					security: []string{},
				})
			}
		}

		// Add request body for POST/PUT operations
		if httpMethod == "post" || httpMethod == "put" {
			if requestBody := g.generateRequestBody(methodAnalysis); requestBody != nil {
				operation.RequestBody = requestBody
			}
		}

		paths[path][httpMethod] = operation
	}

	return paths
}

// Helper functions for generating OpenAPI elements
func generateOperationSummary(analysis *MethodAnalysis) string {
	// Always generate display text instead of using description
	// Extract resource name from the tags
	resourceName := "resource"
	pluralResourceName := "resources"

	if len(analysis.Tags) > 0 {
		tag := analysis.Tags[0]
		// Since tags are now singular, use them directly
		resourceName = tag
		// Get the proper plural form for display
		pluralResourceName = pluralize(resourceName)
	}

	// Convert to proper display format
	displayName := convertResourceToDisplayName(resourceName)
	pluralDisplayName := convertResourceToDisplayName(pluralResourceName)
	article := getIndefiniteArticle(displayName)

	// Extract method name from function name (handle both patterns)
	methodName := strings.TrimSuffix(analysis.Name, "Controller")

	switch {
	case strings.HasSuffix(methodName, "Index"):
		return fmt.Sprintf("List all %s", pluralDisplayName)
	case strings.HasSuffix(methodName, "Show"):
		return fmt.Sprintf("Show details of %s specific %s", article, displayName)
	case strings.HasSuffix(methodName, "Store"):
		// For "Create a new X" pattern, we always use "a" before "new"
		return fmt.Sprintf("Create a new %s", displayName)
	case strings.HasSuffix(methodName, "Update"):
		return fmt.Sprintf("Update an existing %s", displayName)
	case strings.HasSuffix(methodName, "Destroy"):
		return fmt.Sprintf("Delete %s %s", article, displayName)
	default:
		return fmt.Sprintf("%s operation", capitalizeFirst(displayName))
	}
}

func generateOperationID(analysis *MethodAnalysis) string {
	// Extract resource name from the tags (now singular)
	resourceName := "resource"

	if len(analysis.Tags) > 0 {
		tag := analysis.Tags[0]
		// Tags are now singular, use them directly
		resourceName = tag
	}

	// Convert to camelCase for operation IDs
	operationResourceName := strings.ToLower(resourceName[:1]) + resourceName[1:]

	// Extract method name from function name
	methodName := strings.TrimSuffix(analysis.Name, "Controller")

	switch {
	case strings.HasSuffix(methodName, "Index"):
		// For list operations, use dynamic pluralization
		pluralResourceName := pluralize(operationResourceName)
		return fmt.Sprintf("list%s", capitalizeFirst(pluralResourceName))
	case strings.HasSuffix(methodName, "Show"):
		return fmt.Sprintf("get%s", capitalizeFirst(operationResourceName))
	case strings.HasSuffix(methodName, "Store"):
		return fmt.Sprintf("create%s", capitalizeFirst(operationResourceName))
	case strings.HasSuffix(methodName, "Update"):
		return fmt.Sprintf("update%s", capitalizeFirst(operationResourceName))
	case strings.HasSuffix(methodName, "Destroy"):
		return fmt.Sprintf("delete%s", capitalizeFirst(operationResourceName))
	default:
		return strings.ToLower(analysis.Name)
	}
}

func convertParameters(params []*ParameterInfo) []Parameter {
	var result []Parameter
	for _, param := range params {
		result = append(result, Parameter{
			Name:        param.Name,
			In:          param.In,
			Description: param.Description,
			Required:    param.Required,
			Schema: &Schema{
				Type:    param.Type,
				Example: param.Example,
			},
		})
	}

	return result
}

func convertResponses(responses map[string]*ResponseInfo) map[string]Response {
	result := make(map[string]Response)

	for code, resp := range responses {
		schema := resp.Schema

		if schema == nil {
			// Default to generic object if no schema was extracted
			schema = &Schema{Type: "object"}
		}

		// For SuccessResponse, wrap the data schema in the standard response format
		switch resp.Type {
		case "success":
			properties := map[string]*Schema{
				"status": {
					Type:        "string",
					Description: "Response status",
					Example:     "success",
				},
			}

			required := []string{"status"}

			// Only include data field if we have an actual data schema (not nil)
			if resp.Schema != nil {
				properties["data"] = resp.Schema
				required = append(required, "data")
			}

			// Only include message field if we have an actual message
			if resp.Message != "" {
				properties["message"] = &Schema{
					Type:        "string",
					Description: "Response message",
					Example:     resp.Message,
				}
				required = append(required, "message")
			}

			schema = &Schema{
				Type:       "object",
				Properties: properties,
				Required:   required,
			}
		case "error":
			// Standard error response format
			schema = &Schema{
				Type: "object",
				Properties: map[string]*Schema{
					"status": {
						Type:        "string",
						Description: "Response status",
						Example:     "error",
					},
					"message": {
						Type:        "string",
						Description: "Error message",
						Example:     getErrorMessageExample(resp.StatusCode),
					},
				},
				Required: []string{"status", "message"},
			}
		case "validation_error":
			// Validation error response format (422)
			schema = &Schema{
				Type: "object",
				Properties: map[string]*Schema{
					"status": {
						Type:        "string",
						Description: "Response status",
						Example:     "error",
					},
					"message": {
						Type:        "string",
						Description: "Error message",
						Example:     "Error: the request input is invalid",
					},
					"errors": {
						Type:        "object",
						Description: "Validation errors",
					},
				},
				Required: []string{"status", "message", "errors"},
			}
		}

		result[code] = Response{
			Description: resp.Description,
			Content: map[string]MediaType{
				"application/json": {
					Schema: schema,
				},
			},
		}
	}

	return result
}

// getErrorMessageExample returns appropriate example error messages for different status codes
func getErrorMessageExample(statusCode int) string {
	switch statusCode {
	case 400:
		return "Error: invalid input provided"
	case 403:
		return "Forbidden: insufficient permissions"
	case 404:
		return "Error: resource not found"
	case 500:
		return "Error: internal server error"
	default:
		return "Error: request failed"
	}
}

func (g *Generator) generateRequestBody(analysis *MethodAnalysis) *RequestBody {
	// Extract resource name from the tags
	resourceName := "resource"

	if len(analysis.Tags) > 0 {
		tag := analysis.Tags[0]
		// Convert plural tag to singular using proper singularization
		resourceName = singularize(tag)
	}

	// Convert to proper display format
	displayName := convertResourceToDisplayName(resourceName)

	if strings.HasSuffix(analysis.Name, "Store") {
		// Look for request struct in type info
		requestTypeName := analysis.Name + "Request"

		if typeInfo, exists := g.typeInfo[requestTypeName]; exists {
			return &RequestBody{
				Description: fmt.Sprintf("%s creation data", capitalizeFirst(displayName)),
				Required:    true,
				Content: map[string]MediaType{
					"application/json": {
						Schema: g.convertTypeInfoToSchema(typeInfo),
					},
				},
			}
		}
	}

	if strings.HasSuffix(analysis.Name, "Update") {
		// Look for request struct in type info
		requestTypeName := analysis.Name + "Request"
		if typeInfo, exists := g.typeInfo[requestTypeName]; exists {
			return &RequestBody{
				Description: fmt.Sprintf("%s update data", capitalizeFirst(displayName)),
				Required:    true,
				Content: map[string]MediaType{
					"application/json": {
						Schema: g.convertTypeInfoToSchema(typeInfo),
					},
				},
			}
		}
	}

	return nil
}

func (g *Generator) convertTypeInfoToSchema(typeInfo *TypeInfo) *Schema {
	schema := &Schema{
		Type:        typeInfo.Type,
		Description: typeInfo.Description,
		Properties:  make(map[string]*Schema),
	}

	var required []string

	for fieldName, fieldInfo := range typeInfo.Fields {
		jsonName := fieldInfo.JSONName

		if jsonName == "" {
			jsonName = fieldName
		}

		fieldSchema := &Schema{
			Type:        g.mapGoTypeToOpenAPI(fieldInfo.Type),
			Description: fieldInfo.Description,
		}

		// Handle time.Time fields specially
		if fieldInfo.Type == "time.Time" || fieldInfo.Type == "*time.Time" {
			fieldSchema.Type = "string"
			fieldSchema.Format = "date-time"

			if fieldInfo.Description == "" {
				if jsonName == "created_at" {
					fieldSchema.Description = "Creation timestamp"
				} else if jsonName == "updated_at" {
					fieldSchema.Description = "Last update timestamp"
				}
			}

			fieldSchema.Example = "2023-09-20T14:30:00Z"
		}

		// Add validation constraints
		g.applyValidationToSchema(fieldInfo, fieldSchema)

		schema.Properties[jsonName] = fieldSchema

		if fieldInfo.Required {
			required = append(required, jsonName)
		}
	}

	schema.Required = required

	return schema
}

func (g *Generator) mapGoTypeToOpenAPI(goType string) string {
	switch {
	case goType == "string":
		return "string"
	case goType == "int" || goType == "int32" || goType == "int64":
		return "integer"
	case goType == "float32" || goType == "float64":
		return "number"
	case goType == "bool":
		return "boolean"
	case strings.HasPrefix(goType, "array["):
		return "array"
	case strings.HasPrefix(goType, "*"):
		return g.mapGoTypeToOpenAPI(strings.TrimPrefix(goType, "*"))
	default:
		return "object"
	}
}

func (g *Generator) applyValidationToSchema(fieldInfo *FieldInfo, schema *Schema) {
	for rule, value := range fieldInfo.Validation {
		switch rule {
		case "min":
			if minVal, err := strconv.Atoi(value); err == nil {
				_ = minVal // Use the value as needed
			}
		case "max":
			if maxVal, err := strconv.Atoi(value); err == nil {
				_ = maxVal // Use the value as needed
			}
		}
	}
}
