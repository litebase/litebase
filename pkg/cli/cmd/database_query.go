package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/charmbracelet/lipgloss/v2"
	"github.com/google/uuid"
	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/litebase/litebase/pkg/database"
	"github.com/spf13/cobra"
)

func NewDatabaseQueryCmd(config *config.CLIConfiguration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query <database/branch> <statement>",
		Short: "Execute a query on a database",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			databaseName, branchName, err := splitDatabasePath(args[0])

			if err != nil {
				return fmt.Errorf("invalid database path: %w", err)
			}

			statement := args[1]

			transactionID, err := cmd.Flags().GetString("transaction-id")

			if err != nil {
				return fmt.Errorf("failed to get transaction ID: %w", err)
			}

			// Get parameter sets and parameters flags
			parameterSetsFlag, err := cmd.Flags().GetString("parameter-sets")

			if err != nil {
				return fmt.Errorf("failed to get parameter sets: %w", err)
			}

			parametersFlag, err := cmd.Flags().GetString("parameters")

			if err != nil {
				return fmt.Errorf("failed to get parameters: %w", err)
			}

			// Check if both parameters and parameter-sets are provided
			if parameterSetsFlag != "" && parametersFlag != "" {
				return fmt.Errorf("cannot specify both parameters and parameter sets")
			}

			var queries []map[string]any

			if parameterSetsFlag != "" {
				// Handle parameter sets - create multiple queries
				var parameterSets [][]database.QueryStatementParameter

				if err := json.Unmarshal([]byte(parameterSetsFlag), &parameterSets); err != nil {
					return fmt.Errorf("failed to unmarshal parameter sets: %w", err)
				}

				for _, paramSet := range parameterSets {
					// Process each parameter in the set
					for i, param := range paramSet {
						paramType, paramValue := inferParameterType(param.Value)
						param.Type = paramType
						param.Value = paramValue
						paramSet[i] = param
					}

					query := map[string]any{
						"id":         uuid.NewString(),
						"statement":  statement,
						"parameters": paramSet,
					}

					if transactionID != "" {
						query["transaction_id"] = transactionID
					}

					queries = append(queries, query)
				}
			} else {
				// Handle single parameter set or no parameters
				var queryInput = &database.QueryInput{
					Statement: statement,
				}

				if transactionID != "" {
					queryInput.TransactionID = transactionID
				}

				if parametersFlag != "" {
					if err := json.Unmarshal([]byte(parametersFlag), &queryInput.Parameters); err != nil {
						return fmt.Errorf("failed to unmarshal parameters: %w", err)
					}
				}

				for i, param := range queryInput.Parameters {
					paramType, paramValue := inferParameterType(param.Value)
					param.Type = paramType
					param.Value = paramValue
					queryInput.Parameters[i] = param
				}

				query := map[string]any{
					"id":         uuid.NewString(),
					"statement":  queryInput.Statement,
					"parameters": queryInput.Parameters,
				}

				if transactionID != "" {
					query["transaction_id"] = queryInput.TransactionID
				}

				queries = append(queries, query)
			}

			res, apiErrors, err := api.Post(
				config,
				fmt.Sprintf("/v1/databases/%s/branches/%s/query", databaseName, branchName),
				map[string]any{
					"queries": queries,
				},
			)

			if err != nil {
				return err
			}

			if apiErrors != nil {
				return fmt.Errorf("API errors: %v", apiErrors)
			}

			rows := []components.CardRow{}

			if transactionID, ok := res["data"].([]any)[0].(map[string]any)["transaction_id"].(string); ok && transactionID != "" {
				rows = append(rows, components.CardRow{
					Key:   "Transaction ID",
					Value: transactionID,
				})
			}

			if changes, ok := res["data"].([]any)[0].(map[string]any)["changes"].(int64); ok && changes > 0 {
				rows = append(rows, components.CardRow{
					Key:   "Changes",
					Value: fmt.Sprintf("%d", changes),
				})
			}

			if lastInsertRowID, ok := res["data"].([]any)[0].(map[string]any)["last_insert_rowid"].(int64); ok && lastInsertRowID > 0 {
				rows = append(rows, components.CardRow{
					Key:   "Last Insert Row ID",
					Value: fmt.Sprintf("%d", lastInsertRowID),
				})
			}

			if latency, ok := res["data"].([]any)[0].(map[string]any)["latency"].(float64); ok {
				rows = append(rows, components.CardRow{
					Key:   "Latency",
					Value: fmt.Sprintf("%.2f ms", latency),
				})
			}

			if rowCount := res["data"].([]any)[0].(map[string]any)["row_count"].(float64); rowCount > 0 {
				rows = append(rows, components.CardRow{
					Key:   "Row Count",
					Value: fmt.Sprintf("%d", int64(rowCount)),
				})
			}

			var cardContent string

			if rows, ok := res["data"].([]any)[0].(map[string]any)["rows"].([]any); ok && len(rows) > 0 {
				columns, ok := res["data"].([]any)[0].(map[string]any)["columns"].([]any)

				if !ok {
					columns = []any{map[string]any{"name": "Column"}}
				}

				rowData := []map[string]any{}

				for _, row := range rows {
					rowSlice, ok := row.([]any)

					if !ok {
						continue // Skip if row is not a slice
					}

					rowMap := make(map[string]any)

					for i, col := range columns {
						var colName string
						
						// Handle both old string format and new object format
						switch c := col.(type) {
						case string:
							colName = c
						case map[string]any:
							if name, ok := c["name"].(string); ok {
								colName = name
							} else {
								continue // Skip if column name is not available
							}
						default:
							continue // Skip if column format is unknown
						}

						value := rowSlice[i]
						rowMap[colName] = fmt.Sprintf("%v", value) // Convert to string for display
					}

					rowData = append(rowData, rowMap)
				}

				rowJSON, err := json.MarshalIndent(rowData, "", "  ")

				if err != nil {
					return fmt.Errorf("failed to marshal row data to JSON: %w", err)
				}

				cardContent = "```json\n" + string(rowJSON) + "\n```"
			}

			_, err = lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.NewCard(
						components.WithCardTitle("Query"),
						components.WithCardDescription(statement),
						components.WithCardRows(rows),
						components.WithCardContent("Results", cardContent),
					).Render(),
				),
			)

			return err
		},
	}

	cmd.Flags().StringP("output", "o", "json", "Output format (json, table)")
	cmd.Flags().String("parameters", "", "Query parameters in JSON format (positional: [{\"name\":\"param1\", \"value\":\"value1\"}, {\"name\":\"param2\", \"value\":\"value2\"}]")
	cmd.Flags().String("parameter-sets", "", "Multiple sets of query parameters in JSON format (sets: [[{\"name\":\"param1\", \"value\":\"value1\"}, {\"name\":\"param2\", \"value\":\"value2\"}], [{\"name\":\"param1\", \"value\":\"value3\"}, {\"name\":\"param2\", \"value\":\"value4\"}]]")
	cmd.Flags().String("transaction-id", "", "Transaction ID for the query, if the query is part of a transaction")

	return cmd
}

// inferParameterType determines the type of a parameter value and converts it accordingly
func inferParameterType(value any) (string, any) {
	// Try to parse as integer
	switch v := value.(type) {
	case int64:
		return "INTEGER", v
	case float64:
		return "FLOAT", v
	case string:
		return "TEXT", v
	default:
		return "TEXT", v
	}
}
