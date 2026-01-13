package database

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/litebase/litebase/internal/utils"
	"github.com/litebase/litebase/pkg/auth"
)

var (
	// Regular expression to match litebase PRAGMA statements
	litebasePragmaRegex = regexp.MustCompile(`(?i)^\s*PRAGMA\s+litebase_(\w+)\s*(?:=\s*(.+))?\s*$`)
)

// LitebasePragmaHandler handles custom litebase PRAGMA statements
type LitebasePragmaHandler struct {
	connection *DatabaseConnection
	databaseID string
	branchID   string
}

// NewLitebasePragmaHandler creates a new handler for litebase PRAGMAs
func NewLitebasePragmaHandler(connection *DatabaseConnection, databaseID, branchID string) *LitebasePragmaHandler {
	return &LitebasePragmaHandler{
		connection: connection,
		databaseID: databaseID,
		branchID:   branchID,
	}
}

// IsLitebasePragma checks if a SQL statement is a litebase PRAGMA
func IsLitebasePragma(sql string) bool {
	return litebasePragmaRegex.MatchString(strings.TrimSpace(sql))
}

// ParseLitebasePragma extracts the pragma name and value from a SQL statement
func ParseLitebasePragma(sql string) (name string, value string, isSet bool, err error) {
	matches := litebasePragmaRegex.FindStringSubmatch(strings.TrimSpace(sql))

	if matches == nil {
		return "", "", false, errors.New("invalid litebase PRAGMA syntax")
	}

	name = strings.ToLower(matches[1])

	if len(matches) > 2 && matches[2] != "" {
		// It's a SET operation
		value = strings.TrimSpace(matches[2])
		// Remove quotes if present
		value = strings.Trim(value, "'\"")
		isSet = true
	} else {
		// It's a GET operation
		isSet = false
	}

	return name, value, isSet, nil
}

// Execute handles the execution of a litebase PRAGMA statement
func (h *LitebasePragmaHandler) Execute(sql string) (any, error) {
	name, value, isSet, err := ParseLitebasePragma(sql)

	if err != nil {
		return nil, err
	}

	// Get the database and branch
	db, err := h.connection.connectionManager.databaseManager.Get(h.databaseID)

	if err != nil {
		return nil, fmt.Errorf("failed to get database: %w", err)
	}

	branch, err := db.BranchByID(h.branchID)

	if err != nil {
		return nil, fmt.Errorf("failed to get branch: %w", err)
	}

	// Get current settings
	settings, err := branch.GetBranchSettings()

	if err != nil {
		return nil, fmt.Errorf("failed to get branch settings: %w", err)
	}

	if isSet {
		// Handle SET operation
		return nil, h.setPragma(name, value, branch, settings)
	} else {
		// Handle GET operation
		return h.getPragma(name, settings)
	}
}

// setPragma handles setting a litebase PRAGMA value
func (h *LitebasePragmaHandler) setPragma(name, value string, branch *Branch, settings *DatabaseBranchSettings) error {
	// Check authorization for MANAGE privilege when modifying settings
	if h.connection.Credential != nil {
		db, err := h.connection.connectionManager.databaseManager.Get(h.databaseID)

		if err != nil {
			return fmt.Errorf("failed to get database: %w", err)
		}

		resources := []string{
			"database:*",
			fmt.Sprintf("database:%s:branch:*", db.DatabaseID),
			fmt.Sprintf("database:%s:branch:%s", db.DatabaseID, branch.DatabaseBranchID),
		}

		// Check authorization using the appropriate method based on credential type
		switch h.connection.Credential.Type() {
		case auth.CredentialTypeBasicAuth:
			if h.connection.Credential.User() != nil {
				if !h.connection.Credential.User().AuthorizeForResource(resources, []auth.Privilege{auth.DatabasePrivilegeManage}) {
					return fmt.Errorf("user is not authorized to manage database branch settings")
				}
			}
		case auth.CredentialTypeToken:
			if h.connection.Credential.Token() != nil {
				if !h.connection.Credential.Token().AuthorizeForResource(resources, []auth.Privilege{auth.DatabasePrivilegeManage}) {
					return fmt.Errorf("token is not authorized to manage database branch settings")
				}
			}
		case auth.CredentialTypeAccessKey:
			if h.connection.Credential.AccessKey() != nil {
				if !h.connection.Credential.AccessKey().AuthorizeForResource(resources, []auth.Privilege{auth.DatabasePrivilegeManage}) {
					return fmt.Errorf("access key is not authorized to manage database branch settings")
				}
			}
		default:
			return fmt.Errorf("invalid credential type for managing database branch settings")
		}
	}

	switch name {
	case "backups_enabled":
		boolVal, err := parseBool(value)
		if err != nil {
			return fmt.Errorf("invalid boolean value for backups_enabled: %v", err)
		}

		settings.BackupsEnabled = boolVal
	case "backup_interval":
		interval := DatabaseBranchBackupInterval(value)

		if !interval.IsValid() {
			return errors.New("invalid backup interval format")
		}

		settings.BackupInterval = interval
	case "backups_retention_days":
		days, err := strconv.Atoi(value)

		if err != nil || days < 1 {
			return errors.New("backups_retention_days must be a positive integer")
		}

		settings.BackupsRetentionDays = days
	case "incremental_backups_enabled":
		boolVal, err := parseBool(value)

		if err != nil {
			return fmt.Errorf("invalid boolean value for incremental_backups_enabled: %v", err)
		}

		settings.IncrementalBackupsEnabled = boolVal
	case "incremental_backups_retention_days":
		days, err := strconv.Atoi(value)

		if err != nil || days < 1 {
			return errors.New("incremental_backups_retention_days must be a positive integer")
		}

		settings.IncrementalBackupsRetentionDays = days
	case "query_logs_enabled":
		boolVal, err := parseBool(value)

		if err != nil {
			return fmt.Errorf("invalid boolean value for query_logs_enabled: %v", err)
		}

		settings.QueryLogsEnabled = boolVal
	case "query_logs_retention_days":
		days, err := strconv.Atoi(value)

		if err != nil || days < 1 {
			return errors.New("query_logs_retention_days must be a positive integer")
		}

		settings.QueryLogsRetentionDays = days
	case "error_logs_enabled":
		boolVal, err := parseBool(value)

		if err != nil {
			return fmt.Errorf("invalid boolean value for error_logs_enabled: %v", err)
		}

		settings.ErrorLogsEnabled = boolVal
	case "error_logs_retention_days":
		days, err := strconv.Atoi(value)

		if err != nil || days < 1 {
			return errors.New("error_logs_retention_days must be a positive integer")
		}

		settings.ErrorLogsRetentionDays = days
	default:
		return fmt.Errorf("unknown litebase PRAGMA: %s", name)
	}

	// Validate settings before updating
	if settings.BackupsEnabled && settings.BackupInterval == "" {
		return errors.New("backup_interval is required when backups are enabled")
	}

	// Update the settings in the database
	if err := branch.UpdateBranchSettings(settings); err != nil {
		return fmt.Errorf("failed to update branch settings: %w", err)
	}

	return nil
}

// getPragma handles getting a litebase PRAGMA value
func (h *LitebasePragmaHandler) getPragma(name string, settings *DatabaseBranchSettings) (interface{}, error) {
	switch name {
	case "backups_enabled":
		return utils.BoolToInt(settings.BackupsEnabled), nil
	case "backup_interval":
		return string(settings.BackupInterval), nil
	case "backups_retention_days":
		return settings.BackupsRetentionDays, nil
	case "incremental_backups_enabled":
		return utils.BoolToInt(settings.IncrementalBackupsEnabled), nil
	case "incremental_backups_retention_days":
		return settings.IncrementalBackupsRetentionDays, nil
	case "query_logs_enabled":
		return utils.BoolToInt(settings.QueryLogsEnabled), nil
	case "query_logs_retention_days":
		return settings.QueryLogsRetentionDays, nil
	case "error_logs_enabled":
		return utils.BoolToInt(settings.ErrorLogsEnabled), nil
	case "error_logs_retention_days":
		return settings.ErrorLogsRetentionDays, nil
	default:
		return nil, fmt.Errorf("unknown litebase PRAGMA: %s", name)
	}
}

// parseBool parses various boolean representations
func parseBool(value string) (bool, error) {
	value = strings.ToLower(value)
	switch value {
	case "true", "1", "on", "yes":
		return true, nil
	case "false", "0", "off", "no":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean value: %s", value)
	}
}
