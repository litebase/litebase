package openapi

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// OpenAPIMetadata holds the OpenAPI specification metadata for a route
type OpenAPIMetadata struct {
	Summary     string                `json:"summary,omitempty"`
	Description string                `json:"description,omitempty"`
	OperationID string                `json:"operationId,omitempty"`
	Tags        []string              `json:"tags,omitempty"`
	Parameters  []Parameter           `json:"parameters,omitempty"`
	RequestBody *RequestBody          `json:"requestBody,omitempty"`
	Responses   map[string]Response   `json:"responses,omitempty"`
	Security    []SecurityRequirement `json:"security,omitempty"`
}

// Parameter represents an OpenAPI parameter
type Parameter struct {
	Name        string      `json:"name"`
	In          string      `json:"in"` // "query", "header", "path", "cookie"
	Description string      `json:"description,omitempty"`
	Required    bool        `json:"required,omitempty"`
	Schema      *Schema     `json:"schema,omitempty"`
	Example     interface{} `json:"example,omitempty"`
}

// RequestBody represents an OpenAPI request body
type RequestBody struct {
	Description string               `json:"description,omitempty"`
	Content     map[string]MediaType `json:"content"`
	Required    bool                 `json:"required,omitempty"`
}

// MediaType represents an OpenAPI media type
type MediaType struct {
	Schema   *Schema                `json:"schema,omitempty"`
	Example  interface{}            `json:"example,omitempty"`
	Examples map[string]interface{} `json:"examples,omitempty"`
}

// Response represents an OpenAPI response
type Response struct {
	Description string               `json:"description"`
	Headers     map[string]Header    `json:"headers,omitempty"`
	Content     map[string]MediaType `json:"content,omitempty"`
}

// Header represents an OpenAPI header
type Header struct {
	Description string      `json:"description,omitempty"`
	Required    bool        `json:"required,omitempty"`
	Schema      *Schema     `json:"schema,omitempty"`
	Example     interface{} `json:"example,omitempty"`
}

// Schema represents an OpenAPI schema
type Schema struct {
	Type        string             `json:"type,omitempty"`
	Format      string             `json:"format,omitempty"`
	Description string             `json:"description,omitempty"`
	Properties  map[string]*Schema `json:"properties,omitempty"`
	Items       *Schema            `json:"items,omitempty"`
	Required    []string           `json:"required,omitempty"`
	Example     interface{}        `json:"example,omitempty"`
	Enum        []interface{}      `json:"enum,omitempty"`
	Ref         string             `json:"$ref,omitempty"`
}

// SecurityRequirement represents an OpenAPI security requirement
type SecurityRequirement map[string][]string

// OpenAPISpec represents the complete OpenAPI specification
type OpenAPISpec struct {
	OpenAPI    string                `json:"openapi"`
	Info       Info                  `json:"info"`
	Servers    []Server              `json:"servers,omitempty"`
	Paths      map[string]PathItem   `json:"paths"`
	Components *Components           `json:"components,omitempty"`
	Security   []SecurityRequirement `json:"security,omitempty"`
	Tags       []Tag                 `json:"tags,omitempty"`
}

// Info represents OpenAPI info object
type Info struct {
	Title          string   `json:"title"`
	Description    string   `json:"description,omitempty"`
	Version        string   `json:"version"`
	TermsOfService string   `json:"termsOfService,omitempty"`
	Contact        *Contact `json:"contact,omitempty"`
	License        *License `json:"license,omitempty"`
}

// Contact represents OpenAPI contact information
type Contact struct {
	Name  string `json:"name,omitempty"`
	URL   string `json:"url,omitempty"`
	Email string `json:"email,omitempty"`
}

// License represents OpenAPI license information
type License struct {
	Name string `json:"name"`
	URL  string `json:"url,omitempty"`
}

// Server represents an OpenAPI server
type Server struct {
	URL         string                    `json:"url"`
	Description string                    `json:"description,omitempty"`
	Variables   map[string]ServerVariable `json:"variables,omitempty"`
}

// ServerVariable represents an OpenAPI server variable
type ServerVariable struct {
	Enum        []string `json:"enum,omitempty"`
	Default     string   `json:"default"`
	Description string   `json:"description,omitempty"`
}

// PathItem represents an OpenAPI path item
type PathItem struct {
	Summary     string      `json:"summary,omitempty"`
	Description string      `json:"description,omitempty"`
	Get         *Operation  `json:"get,omitempty"`
	Put         *Operation  `json:"put,omitempty"`
	Post        *Operation  `json:"post,omitempty"`
	Delete      *Operation  `json:"delete,omitempty"`
	Options     *Operation  `json:"options,omitempty"`
	Head        *Operation  `json:"head,omitempty"`
	Patch       *Operation  `json:"patch,omitempty"`
	Trace       *Operation  `json:"trace,omitempty"`
	Parameters  []Parameter `json:"parameters,omitempty"`
}

// Operation represents an OpenAPI operation
type Operation struct {
	Tags        []string               `json:"tags,omitempty"`
	Summary     string                 `json:"summary,omitempty"`
	Description string                 `json:"description,omitempty"`
	OperationID string                 `json:"operationId,omitempty"`
	Parameters  []Parameter            `json:"parameters,omitempty"`
	RequestBody *RequestBody           `json:"requestBody,omitempty"`
	Responses   map[string]Response    `json:"responses"`
	Callbacks   map[string]interface{} `json:"callbacks,omitempty"`
	Deprecated  bool                   `json:"deprecated,omitempty"`
	Security    []SecurityRequirement  `json:"security,omitempty"`
	Servers     []Server               `json:"servers,omitempty"`
}

// Components represents OpenAPI components
type Components struct {
	Schemas         map[string]*Schema        `json:"schemas,omitempty"`
	Responses       map[string]Response       `json:"responses,omitempty"`
	Parameters      map[string]Parameter      `json:"parameters,omitempty"`
	Examples        map[string]interface{}    `json:"examples,omitempty"`
	RequestBodies   map[string]RequestBody    `json:"requestBodies,omitempty"`
	Headers         map[string]Header         `json:"headers,omitempty"`
	SecuritySchemes map[string]SecurityScheme `json:"securitySchemes,omitempty"`
	Links           map[string]interface{}    `json:"links,omitempty"`
	Callbacks       map[string]interface{}    `json:"callbacks,omitempty"`
}

// SecurityScheme represents an OpenAPI security scheme
type SecurityScheme struct {
	Type             string      `json:"type"`
	Description      string      `json:"description,omitempty"`
	Name             string      `json:"name,omitempty"`
	In               string      `json:"in,omitempty"`
	Scheme           string      `json:"scheme,omitempty"`
	BearerFormat     string      `json:"bearerFormat,omitempty"`
	Flows            *OAuthFlows `json:"flows,omitempty"`
	OpenIDConnectURL string      `json:"openIdConnectUrl,omitempty"`
}

// OAuthFlows represents OAuth flows
type OAuthFlows struct {
	Implicit          *OAuthFlow `json:"implicit,omitempty"`
	Password          *OAuthFlow `json:"password,omitempty"`
	ClientCredentials *OAuthFlow `json:"clientCredentials,omitempty"`
	AuthorizationCode *OAuthFlow `json:"authorizationCode,omitempty"`
}

// OAuthFlow represents an OAuth flow
type OAuthFlow struct {
	AuthorizationURL string            `json:"authorizationUrl,omitempty"`
	TokenURL         string            `json:"tokenUrl,omitempty"`
	RefreshURL       string            `json:"refreshUrl,omitempty"`
	Scopes           map[string]string `json:"scopes"`
}

// Tag represents an OpenAPI tag
type Tag struct {
	Name         string                 `json:"name"`
	Description  string                 `json:"description,omitempty"`
	ExternalDocs *ExternalDocumentation `json:"externalDocs,omitempty"`
}

// ExternalDocumentation represents external documentation
type ExternalDocumentation struct {
	Description string `json:"description,omitempty"`
	URL         string `json:"url"`
}

// RouteAnalyzer analyzes route handlers and middleware to extract response information
type RouteAnalyzer struct {
	fileSet *token.FileSet
}

// NewRouteAnalyzer creates a new route analyzer
func NewRouteAnalyzer() *RouteAnalyzer {
	return &RouteAnalyzer{
		fileSet: token.NewFileSet(),
	}
}

// AnalyzeHandler analyzes a handler function to extract possible response codes and types
func (ra *RouteAnalyzer) AnalyzeHandler(handlerName string, sourceDir string) (map[string]Response, error) {
	responses := make(map[string]Response)

	// Try to find and parse the handler function
	if handlerFile, err := ra.findHandlerFile(handlerName, sourceDir); err == nil {
		if specificResponses, err := ra.parseHandlerResponses(handlerFile, handlerName); err == nil {
			for code, response := range specificResponses {
				responses[code] = response
			}
		}
	}

	// If no specific responses found, add defaults
	if len(responses) == 0 {
		responses = ra.getDefaultResponses()
	}

	return responses, nil
}

// findHandlerFile finds the file containing the handler function
func (ra *RouteAnalyzer) findHandlerFile(handlerName string, sourceDir string) (string, error) {
	// Look for controller files in the HTTP package
	controllerFile := fmt.Sprintf("%s/pkg/http/%s.go", sourceDir, convertHandlerNameToFileName(handlerName))
	return controllerFile, nil
}

// convertHandlerNameToFileName converts a handler function name to likely file name
func convertHandlerNameToFileName(handlerName string) string {
	// Convert "UserControllerIndex" to "user_controller"
	// This is a simplified approach - you might need to enhance this
	name := handlerName
	if strings.HasSuffix(name, "Controller") || strings.Contains(name, "Controller") {
		// Extract the base name before "Controller"
		parts := strings.Split(name, "Controller")
		if len(parts) > 0 {
			base := parts[0]
			// Convert CamelCase to snake_case
			result := ""
			for i, r := range base {
				if i > 0 && r >= 'A' && r <= 'Z' {
					result += "_"
				}
				result += strings.ToLower(string(r))
			}
			return result + "_controller"
		}
	}
	return strings.ToLower(name)
}

// getDefaultResponses returns default responses for any endpoint
func (ra *RouteAnalyzer) getDefaultResponses() map[string]Response {
	return map[string]Response{
		"200": {
			Description: "Successful operation",
			Content: map[string]MediaType{
				"application/json": {
					Schema: &Schema{
						Ref: "#/components/schemas/SuccessResponse",
					},
				},
			},
		},
		"400": {
			Description: "Bad request - invalid input or parameters",
			Content: map[string]MediaType{
				"application/json": {
					Schema: &Schema{
						Ref: "#/components/schemas/ErrorResponse",
					},
				},
			},
		},
		"401": {
			Description: "Unauthorized - authentication required",
			Content: map[string]MediaType{
				"application/json": {
					Schema: &Schema{
						Ref: "#/components/schemas/ErrorResponse",
					},
				},
			},
		},
		"500": {
			Description: "Internal server error",
			Content: map[string]MediaType{
				"application/json": {
					Schema: &Schema{
						Ref: "#/components/schemas/ErrorResponse",
					},
				},
			},
		},
	}
}

// parseHandlerResponses parses a handler function to extract response codes
func (ra *RouteAnalyzer) parseHandlerResponses(filename string, handlerName string) (map[string]Response, error) {
	responses := make(map[string]Response)

	// Read the file
	content, err := os.ReadFile(filename)

	if err != nil {
		return responses, err
	}

	// Parse the file
	node, err := parser.ParseFile(ra.fileSet, filename, content, parser.ParseComments)
	if err != nil {
		return responses, err
	}

	// Find the handler function
	ast.Inspect(node, func(n ast.Node) bool {
		if fn, ok := n.(*ast.FuncDecl); ok && fn.Name.Name == handlerName {
			// Analyze the function body for response patterns
			ra.analyzeFunctionBody(fn, responses)
		}
		return true
	})

	return responses, nil
}

// analyzeFunctionBody analyzes function body for response patterns
func (ra *RouteAnalyzer) analyzeFunctionBody(fn *ast.FuncDecl, responses map[string]Response) {
	if fn.Body == nil {
		return
	}

	// Look for Response{StatusCode: xxx} patterns and function calls that return responses
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.CompositeLit:
			// Handle Response{StatusCode: xxx, Body: {...}} patterns
			if ra.isResponseLiteral(node) {
				ra.extractResponseFromLiteral(node, responses)
			}
		case *ast.CallExpr:
			// Handle JsonResponse(), ForbiddenResponse(), etc. function calls
			ra.extractResponseFromCall(node, responses)
		case *ast.ReturnStmt:
			// Handle return statements with response calls
			for _, result := range node.Results {
				if call, ok := result.(*ast.CallExpr); ok {
					ra.extractResponseFromCall(call, responses)
				}
			}
		}
		return true
	})
}

// isResponseLiteral checks if a composite literal is a Response struct
func (ra *RouteAnalyzer) isResponseLiteral(cl *ast.CompositeLit) bool {
	if ident, ok := cl.Type.(*ast.Ident); ok {
		return ident.Name == "Response"
	}
	return false
}

// extractResponseFromLiteral extracts response info from Response{...} literal
func (ra *RouteAnalyzer) extractResponseFromLiteral(cl *ast.CompositeLit, responses map[string]Response) {
	statusCode := ""
	description := ""

	for _, elt := range cl.Elts {
		if kv, ok := elt.(*ast.KeyValueExpr); ok {
			if key, ok := kv.Key.(*ast.Ident); ok {
				switch key.Name {
				case "StatusCode":
					if lit, ok := kv.Value.(*ast.BasicLit); ok && lit.Kind == token.INT {
						statusCode = lit.Value
					}
				case "Body":
					// Could analyze body structure here for more detailed schemas
					description = ra.getDefaultResponseDescription(parseStatusCode(statusCode))
				}
			}
		}
	}

	if statusCode != "" {
		responses[statusCode] = Response{
			Description: description,
			Content: map[string]MediaType{
				"application/json": {
					Schema: &Schema{
						Type: "object",
					},
				},
			},
		}
	}
}

// extractResponseFromCall extracts response info from function calls like JsonResponse()
func (ra *RouteAnalyzer) extractResponseFromCall(call *ast.CallExpr, responses map[string]Response) {
	if ident, ok := call.Fun.(*ast.Ident); ok {
		switch ident.Name {
		case "JsonResponse":
			// JsonResponse(body, statusCode, headers)
			if len(call.Args) >= 2 {
				if lit, ok := call.Args[1].(*ast.BasicLit); ok && lit.Kind == token.INT {
					statusCode := lit.Value
					responses[statusCode] = Response{
						Description: ra.getDefaultResponseDescription(parseStatusCode(statusCode)),
						Content: map[string]MediaType{
							"application/json": {
								Schema: &Schema{
									Type: "object",
								},
							},
						},
					}
				}
			}
		case "ForbiddenResponse":
			responses["403"] = Response{
				Description: "Forbidden - insufficient permissions",
				Content: map[string]MediaType{
					"application/json": {
						Schema: &Schema{
							Ref: "#/components/schemas/ErrorResponse",
						},
					},
				},
			}
		case "UnauthorizedResponse":
			responses["401"] = Response{
				Description: "Unauthorized - authentication required",
				Content: map[string]MediaType{
					"application/json": {
						Schema: &Schema{
							Ref: "#/components/schemas/ErrorResponse",
						},
					},
				},
			}
		case "BadRequestResponse":
			responses["400"] = Response{
				Description: "Bad request - invalid input",
				Content: map[string]MediaType{
					"application/json": {
						Schema: &Schema{
							Ref: "#/components/schemas/ErrorResponse",
						},
					},
				},
			}
		case "NotFoundResponse":
			responses["404"] = Response{
				Description: "Resource not found",
				Content: map[string]MediaType{
					"application/json": {
						Schema: &Schema{
							Ref: "#/components/schemas/ErrorResponse",
						},
					},
				},
			}
		}
	}
}

// parseStatusCode converts string status code to int
func parseStatusCode(statusCodeStr string) int {
	if code, err := strconv.Atoi(statusCodeStr); err == nil {
		return code
	}
	return 200
}

// getDefaultResponseDescription returns a default description for a status code
func (ra *RouteAnalyzer) getDefaultResponseDescription(statusCode int) string {
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
	case 408:
		return "Request timeout"
	case 409:
		return "Conflict"
	case 422:
		return "Validation error"
	case 500:
		return "Internal server error"
	default:
		return fmt.Sprintf("Response with status code %d", statusCode)
	}
}

// ExtractPathParameters extracts path parameters from a route path
func ExtractPathParameters(path string) []Parameter {
	var parameters []Parameter

	// Find parameters in the format {paramName}
	re := regexp.MustCompile(`\{([^}]+)\}`)
	matches := re.FindAllStringSubmatch(path, -1)

	for _, match := range matches {
		if len(match) > 1 {
			paramName := match[1]
			parameters = append(parameters, Parameter{
				Name:        paramName,
				In:          "path",
				Description: fmt.Sprintf("The %s parameter", paramName),
				Required:    true,
				Schema: &Schema{
					Type: "string",
				},
			})
		}
	}

	return parameters
}

// ConvertPathToOpenAPI converts a path with {param} format to OpenAPI format
func ConvertPathToOpenAPI(path string) string {
	// OpenAPI uses {param} format, which is already what we have
	return path
}
