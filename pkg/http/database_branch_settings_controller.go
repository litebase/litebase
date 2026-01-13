package http

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/cluster/messages"
	"github.com/litebase/litebase/pkg/database"
)

// Response type for showing branch settings
type DatabaseBranchSettingsShowResponse *database.DatabaseBranchSettings

// Show the settings for a specific database branch
func DatabaseBranchSettingsControllerShow(ctx context.Context, request *Request) Response {
	databaseKey, errResponse := request.DatabaseKey()

	if !errResponse.IsEmpty() {
		return errResponse
	}

	db, err := request.databaseManager.Get(databaseKey.DatabaseID)

	if err != nil {
		if err == sql.ErrNoRows {
			return NotFoundResponse(errors.New("database not found"))
		}

		return BadRequestResponse(err)
	}

	// Get the branch by name
	branch, err := db.Branch(databaseKey.DatabaseBranchName)

	if err != nil {
		if err == sql.ErrNoRows {
			return NotFoundResponse(errors.New("branch not found"))
		}

		slog.Error("Failed to retrieve database branch", "error", err, "databaseId", db.DatabaseID, "branchName", databaseKey.DatabaseBranchName)

		return BadRequestResponse(err)
	}

	// Authorize the request
	err = request.Authorize(
		[]string{
			"database:*",
			fmt.Sprintf("database:%s:branch:*", db.DatabaseID),
			fmt.Sprintf("database:%s:branch:%s", db.DatabaseID, branch.DatabaseBranchID),
		},
		[]auth.Privilege{auth.DatabasePrivilegeShow},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	// Get the branch settings
	settings, err := branch.GetBranchSettings()

	if err != nil {
		slog.Error("Failed to retrieve branch settings", "error", err, "branchId", branch.ID)

		return ServerErrorResponse(err)
	}

	return SuccessResponse(
		"Successfully retrieved branch settings.",
		DatabaseBranchSettingsShowResponse(settings),
		200,
	)
}

// Request payload for updating branch settings
type DatabaseBranchSettingsUpdateRequest struct {
	BackupsEnabled                  bool                                    `json:"backupsEnabled"`
	BackupInterval                  database.DatabaseBranchBackupInterval   `json:"backupInterval" validate:"omitempty,required_if=BackupsEnabled true,validateFn=IsValid"`
	BackupsRetentionDays            int                                     `json:"backupsRetentionDays" validate:"required_if=BackupsEnabled true,number,min=1"`
	DefaultPragmas                  *database.DatabaseDefaultPragmaSettings `json:"defaultPragmas" validate:"omitempty"`
	ErrorLogsEnabled                bool                                    `json:"errorLogsEnabled"`
	ErrorLogsRetentionDays          int                                     `json:"errorLogsRetentionDays" validate:"required_if=ErrorLogsEnabled true,number,min=1"`
	IncrementalBackupsEnabled       bool                                    `json:"incrementalBackupsEnabled"`
	IncrementalBackupsRetentionDays int                                     `json:"incrementalBackupsRetentionDays" validate:"required_if=IncrementalBackupsEnabled true,number,min=1"`
	QueryLogsEnabled                bool                                    `json:"queryLogsEnabled"`
	QueryLogsRetentionDays          int                                     `json:"queryLogsRetentionDays" validate:"required_if=QueryLogsEnabled true,number,min=1"`
}

// Response type for updating branch settings
type DatabaseBranchSettingsUpdateResponse *database.DatabaseBranchSettings

// Update the settings for a specific database branch
func DatabaseBranchSettingsControllerUpdate(ctx context.Context, request *Request) Response {
	if !request.cluster.Node().IsPrimary() {
		return ForbiddenResponse(errors.New("node is not primary"))
	}

	databaseKey, errResponse := request.DatabaseKey()

	if !errResponse.IsEmpty() {
		return errResponse
	}

	db, err := request.databaseManager.Get(databaseKey.DatabaseID)

	if err != nil {
		if err == sql.ErrNoRows {
			return NotFoundResponse(errors.New("database not found"))
		}

		return BadRequestResponse(err)
	}

	// Get the branch by name
	branch, err := db.Branch(databaseKey.DatabaseBranchName)

	if err != nil {
		if err == sql.ErrNoRows {
			return NotFoundResponse(errors.New("branch not found"))
		}

		slog.Error("Failed to retrieve database branch", "error", err, "databaseId", db.DatabaseID, "branchName", databaseKey.DatabaseBranchName)
		return BadRequestResponse(err)
	}

	// Authorize the request
	err = request.Authorize(
		[]string{
			"database:*",
			fmt.Sprintf("database:%s:branch:*", db.DatabaseID),
			fmt.Sprintf("database:%s:branch:%s", db.DatabaseID, branch.DatabaseBranchID),
		},
		[]auth.Privilege{auth.DatabasePrivilegeManage},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	// Parse the request body
	input, err := request.Input(&DatabaseBranchSettingsUpdateRequest{})

	if err != nil {
		return BadRequestResponse(err)
	}

	req, ok := input.(*DatabaseBranchSettingsUpdateRequest)

	if !ok {
		return ServerErrorResponse(errors.New("invalid request format"))
	}

	// Validate the request
	validationErrors := request.Validate(req, map[string]string{
		"backupInterval.validateFn":                "Invalid backup interval format",
		"backupsRetentionDays.min":                 "Backups retention days must be at least 1",
		"errorLogsRetentionDays.min":               "Error logs retention days must be at least 1",
		"incrementalBackupsRetentionDays.min":      "Incremental backups retention days must be at least 1",
		"queryLogsRetentionDays.min":               "Query logs retention days must be at least 1",
		"foreignKeys.required":                     "Foreign keys setting is required when default pragmas are provided",
		"foreignKeys.oneof":                        "Foreign keys must be either 'ON' or 'OFF'",
	})

	if validationErrors != nil {
		return ValidationErrorResponse(validationErrors)
	}

	// Custom validation: backupInterval is required when backups are enabled
	if req.BackupsEnabled {
		if req.BackupInterval == "" {
			return ValidationErrorResponse(map[string][]string{
				"backupInterval": {"Backup interval is required when backups are enabled"},
			})
		}

		if !req.BackupInterval.IsValid() {
			return ValidationErrorResponse(map[string][]string{
				"backupInterval": {"Invalid backup interval format"},
			})
		}
	}

	// Create new settings from request
	newSettings := &database.DatabaseBranchSettings{
		BackupsEnabled:                  req.BackupsEnabled,
		BackupInterval:                  req.BackupInterval,
		BackupsRetentionDays:            req.BackupsRetentionDays,
		DefaultPragmas:                  req.DefaultPragmas,
		ErrorLogsEnabled:                req.ErrorLogsEnabled,
		ErrorLogsRetentionDays:          req.ErrorLogsRetentionDays,
		IncrementalBackupsEnabled:       req.IncrementalBackupsEnabled,
		IncrementalBackupsRetentionDays: req.IncrementalBackupsRetentionDays,
		QueryLogsEnabled:                req.QueryLogsEnabled,
		QueryLogsRetentionDays:          req.QueryLogsRetentionDays,
	}

	// Update the settings
	err = branch.UpdateBranchSettings(newSettings)

	if err != nil {
		slog.Error("Failed to update branch settings", "error", err, "branchId", branch.ID)
		return ServerErrorResponse(err)
	}

	// Reload the settings from the database to ensure consistency
	updatedSettings, err := branch.GetBranchSettings()

	if err != nil {
		slog.Error("Failed to reload branch settings", "error", err, "branchId", branch.ID)
		return ServerErrorResponse(err)
	}

	// Update the branch's in-memory settings
	branch.Settings = updatedSettings

	// Broadcast the settings update to all nodes in the cluster
	defer func() {
		_, errMap := request.cluster.Node().Primary().Publish(messages.NodeMessage{
			Data: messages.DatabaseBranchSettingsUpdated{
				DatabaseID:       db.DatabaseID,
				DatabaseBranchID: branch.DatabaseBranchID,
			},
		})

		if errMap != nil {
			slog.Error("Failed to broadcast database branch settings update", "error", errMap)
		}
	}()

	return SuccessResponse(
		"Branch settings updated successfully",
		DatabaseBranchSettingsUpdateResponse(updatedSettings),
		200,
	)
}
