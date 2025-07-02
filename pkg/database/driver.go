package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/litebase/litebase/pkg/sqlite3"
)

var (
	// Ensure our types implement the required interfaces
	_ driver.Driver             = (*LitebaseSQLDriver)(nil)
	_ driver.Conn               = (*LitebaseConn)(nil)
	_ driver.Pinger             = (*LitebaseConn)(nil)
	_ driver.Stmt               = (*LitebaseStmt)(nil)
	_ driver.Rows               = (*LitebaseRows)(nil)
	_ driver.Result             = (*LitebaseResult)(nil)
	_ driver.Tx                 = (*LitebaseTx)(nil)
	_ driver.ConnBeginTx        = (*LitebaseConn)(nil)
	_ driver.ConnPrepareContext = (*LitebaseConn)(nil)
	_ driver.ExecerContext      = (*LitebaseConn)(nil)
	_ driver.QueryerContext     = (*LitebaseConn)(nil)
	_ driver.StmtExecContext    = (*LitebaseStmt)(nil)
	_ driver.StmtQueryContext   = (*LitebaseStmt)(nil)
)

// Global driver instance that can have its connection manager updated
var globalDriverMutex sync.RWMutex
var globalDriver *LitebaseSQLDriver
var driverRegistered bool
var registeredDriverName string

// GlobalDriverWrapper wraps the global driver to allow connection manager updates
type GlobalDriverWrapper struct {
}

func (d *GlobalDriverWrapper) Open(name string) (driver.Conn, error) {
	globalDriverMutex.RLock()
	defer globalDriverMutex.RUnlock()

	if globalDriver == nil {
		return nil, fmt.Errorf("litebase driver not initialized")
	}

	return globalDriver.Open(name)
}

// UpdateGlobalConnectionManager updates the connection manager for the global driver
func UpdateGlobalConnectionManager(connectionManager *ConnectionManager) {
	globalDriverMutex.Lock()
	defer globalDriverMutex.Unlock()

	globalDriver = NewLitebaseSQLDriver(connectionManager)
}

// LitebaseSQLDriver implements driver.Driver
type LitebaseSQLDriver struct {
	connectionManager *ConnectionManager
}

// NewLitebaseSQLDriver creates a new driver instance
func NewLitebaseSQLDriver(connectionManager *ConnectionManager) *LitebaseSQLDriver {
	return &LitebaseSQLDriver{
		connectionManager: connectionManager,
	}
}

// Open returns a new connection to the database.
// The name is a string in the format "database_id/branch_id"
func (d *LitebaseSQLDriver) Open(name string) (driver.Conn, error) {
	parts := strings.Split(name, "/")

	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid database name format, expected 'database_id/branch_id', got '%s'", name)
	}

	databaseId := parts[0]
	branchId := parts[1]

	con, err := d.connectionManager.Get(databaseId, branchId)

	if err != nil {
		return nil, fmt.Errorf("failed to create database connection: %w", err)
	}

	return &LitebaseConn{
		connectionManager: d.connectionManager,
		conn:              con,
	}, nil
}

// LitebaseConn implements driver.Conn
type LitebaseConn struct {
	connectionManager *ConnectionManager
	conn              *ClientConnection
}

// Prepare returns a prepared statement, bound to this connection
func (c *LitebaseConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// PrepareContext returns a prepared statement, bound to this connection
func (c *LitebaseConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if c.conn.GetConnection().Closed() {
		return nil, driver.ErrBadConn
	}

	stmt, err := c.conn.GetConnection().Prepare(ctx, query)
	if err != nil {
		return nil, err
	}

	return &LitebaseStmt{
		conn:  c.conn.GetConnection(),
		stmt:  stmt,
		query: query,
	}, nil
}

// Close invalidates and potentially stops any current prepared statements and transactions
func (c *LitebaseConn) Close() error {
	if c.conn == nil {
		return nil
	}

	c.connectionManager.Release(c.conn)

	c.conn = nil

	return nil
}

// Ping verifies a connection to the database is still alive, establishing a connection if necessary
func (c *LitebaseConn) Ping(ctx context.Context) error {
	if c.conn == nil {
		return driver.ErrBadConn
	}

	// Check if the connection is closed
	if c.conn.GetConnection().Closed() {
		return driver.ErrBadConn
	}

	// Execute a simple query to verify the connection is working
	// Use a query that should work on any SQLite database
	_, err := c.conn.GetConnection().Exec("SELECT 1", nil)
	if err != nil {
		return err
	}

	return nil
}

func (c *LitebaseConn) PingContext(ctx context.Context) error {
	return c.Ping(ctx)
}

// Begin starts and returns a new transaction
func (c *LitebaseConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx starts and returns a new transaction with the given options
func (c *LitebaseConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.conn.GetConnection().Closed() {
		return nil, driver.ErrBadConn
	}

	// Check isolation level support
	if opts.Isolation != driver.IsolationLevel(sql.LevelDefault) &&
		opts.Isolation != driver.IsolationLevel(sql.LevelSerializable) {

		return nil, fmt.Errorf("unsupported isolation level: %v", opts.Isolation)
	}

	if opts.ReadOnly {
		// Start a read-only transaction
		_, err := c.conn.GetConnection().Exec("BEGIN", nil)

		if err != nil {
			return nil, err
		}
	} else {
		// Start a read-write transaction
		_, err := c.conn.GetConnection().Exec("BEGIN", nil)

		if err != nil {
			return nil, err
		}
	}

	return &LitebaseTx{
		conn: c.conn.GetConnection(),
	}, nil
}

// ExecContext executes a query without returning any rows
func (c *LitebaseConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.conn.GetConnection().Closed() {
		return nil, driver.ErrBadConn
	}

	params, err := convertNamedValuesToParameters(args)

	if err != nil {
		return nil, err
	}

	result, err := c.conn.GetConnection().Exec(query, params)

	if err != nil {
		return nil, err
	}

	return &LitebaseResult{result: result, conn: c.conn.GetConnection()}, nil
}

// QueryContext executes a query that may return rows
func (c *LitebaseConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.conn.GetConnection().Closed() {
		return nil, driver.ErrBadConn
	}

	params, err := convertNamedValuesToParameters(args)

	if err != nil {
		return nil, err
	}

	result, err := c.conn.GetConnection().Exec(query, params)

	if err != nil {
		return nil, err
	}

	return &LitebaseRows{
		result:      result,
		currentRow:  -1,
		columnNames: result.Columns,
	}, nil
}

// LitebaseStmt implements driver.Stmt
type LitebaseStmt struct {
	conn  *DatabaseConnection
	stmt  Statement
	query string
}

// Close closes the statement
func (s *LitebaseStmt) Close() error {
	// SQLite statements are automatically finalized when the connection closes
	return nil
}

// NumInput returns the number of placeholder parameters
func (s *LitebaseStmt) NumInput() int {
	// Return -1 to indicate we don't know the number of parameters
	// This allows the driver to accept any number of parameters
	return -1
}

// Exec executes a query that doesn't return rows
func (s *LitebaseStmt) Exec(args []driver.Value) (driver.Result, error) {
	namedArgs := make([]driver.NamedValue, len(args))

	for i, arg := range args {
		namedArgs[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   arg,
		}
	}

	return s.ExecContext(context.Background(), namedArgs)
}

// ExecContext executes a query that doesn't return rows
func (s *LitebaseStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if s.conn.Closed() {
		return nil, driver.ErrBadConn
	}

	params, err := convertNamedValuesToParameters(args)

	if err != nil {
		return nil, err
	}

	result, err := s.conn.Exec(s.query, params)

	if err != nil {
		return nil, err
	}

	return &LitebaseResult{result: result, conn: s.conn}, nil
}

// Query executes a query that may return rows
func (s *LitebaseStmt) Query(args []driver.Value) (driver.Rows, error) {
	namedArgs := make([]driver.NamedValue, len(args))

	for i, arg := range args {
		namedArgs[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   arg,
		}
	}

	return s.QueryContext(context.Background(), namedArgs)
}

// QueryContext executes a query that may return rows
func (s *LitebaseStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if s.conn.Closed() {
		return nil, driver.ErrBadConn
	}

	params, err := convertNamedValuesToParameters(args)

	if err != nil {
		return nil, err
	}

	result, err := s.conn.Exec(s.query, params)

	if err != nil {
		return nil, err
	}

	return &LitebaseRows{
		result:      result,
		currentRow:  -1,
		columnNames: result.Columns,
	}, nil
}

// LitebaseRows implements driver.Rows
type LitebaseRows struct {
	result      *sqlite3.Result
	currentRow  int
	columnNames []string
}

// Columns returns the names of the columns
func (r *LitebaseRows) Columns() []string {
	return r.columnNames
}

// Close closes the rows iterator
func (r *LitebaseRows) Close() error {
	// No cleanup needed for our result structure
	return nil
}

// Next is called to populate the next row of data into the provided slice
func (r *LitebaseRows) Next(dest []driver.Value) error {
	if r.currentRow >= len(r.result.Rows)-1 {
		return io.EOF
	}

	r.currentRow++
	row := r.result.Rows[r.currentRow]

	if len(dest) != len(row) {
		return fmt.Errorf("expected %d destination values, got %d", len(row), len(dest))
	}

	for i, col := range row {
		dest[i] = convertColumnToDriverValue(col)
	}

	return nil
}

// LitebaseResult implements driver.Result
type LitebaseResult struct {
	result *sqlite3.Result
	conn   *DatabaseConnection
}

// LastInsertId returns the database's auto-generated ID after an INSERT
func (r *LitebaseResult) LastInsertId() (int64, error) {
	if r.conn == nil || r.conn.Closed() {
		return 0, driver.ErrBadConn
	}
	return r.conn.LastInsertRowID(), nil
}

// RowsAffected returns the number of rows affected by the query
func (r *LitebaseResult) RowsAffected() (int64, error) {
	if r.conn == nil || r.conn.Closed() {
		return 0, driver.ErrBadConn
	}

	return r.conn.Changes(), nil
}

// LitebaseTx implements driver.Tx
type LitebaseTx struct {
	conn *DatabaseConnection
}

// Commit commits the transaction
func (tx *LitebaseTx) Commit() error {
	if tx.conn.Closed() {
		return driver.ErrBadConn
	}

	_, err := tx.conn.Exec("COMMIT", nil)

	return err
}

// Rollback aborts the transaction
func (tx *LitebaseTx) Rollback() error {
	if tx.conn.Closed() {
		return driver.ErrBadConn
	}

	_, err := tx.conn.Exec("ROLLBACK", nil)

	return err
}

// Helper functions

// convertNamedValuesToParameters converts driver.NamedValue slice to StatementParameter slice
func convertNamedValuesToParameters(args []driver.NamedValue) ([]sqlite3.StatementParameter, error) {
	params := make([]sqlite3.StatementParameter, len(args))

	for i, arg := range args {
		param, err := convertDriverValueToParameter(arg.Value)

		if err != nil {
			return nil, fmt.Errorf("convert argument %d: %w", i, err)
		}

		params[i] = param
	}

	return params, nil
}

// convertDriverValueToParameter converts a driver.Value to StatementParameter
func convertDriverValueToParameter(value driver.Value) (sqlite3.StatementParameter, error) {
	if value == nil {
		return sqlite3.StatementParameter{
			Type:  sqlite3.ParameterTypeNull,
			Value: nil,
		}, nil
	}

	switch v := value.(type) {
	case int64:
		return sqlite3.StatementParameter{
			Type:  sqlite3.ParameterTypeInteger,
			Value: v,
		}, nil
	case float64:
		return sqlite3.StatementParameter{
			Type:  sqlite3.ParameterTypeFloat,
			Value: v,
		}, nil
	case bool:
		var val int64
		if v {
			val = 1
		}

		return sqlite3.StatementParameter{
			Type:  sqlite3.ParameterTypeInteger,
			Value: val,
		}, nil
	case []byte:
		return sqlite3.StatementParameter{
			Type:  sqlite3.ParameterTypeBlob,
			Value: v,
		}, nil
	case string:
		return sqlite3.StatementParameter{
			Type:  sqlite3.ParameterTypeText,
			Value: []byte(v),
		}, nil
	case time.Time:
		return sqlite3.StatementParameter{
			Type:  sqlite3.ParameterTypeText,
			Value: []byte(v.Format(time.RFC3339Nano)),
		}, nil
	case *time.Time:
		if v == nil {
			return sqlite3.StatementParameter{
				Type:  sqlite3.ParameterTypeNull,
				Value: nil,
			}, nil
		}

		return sqlite3.StatementParameter{
			Type:  sqlite3.ParameterTypeText,
			Value: []byte(v.Format(time.RFC3339Nano)),
		}, nil
	default:
		return sqlite3.StatementParameter{}, fmt.Errorf("unsupported parameter type %T", value)
	}
}

// convertColumnToDriverValue converts a sqlite3.Column to driver.Value
func convertColumnToDriverValue(col *sqlite3.Column) driver.Value {
	if col == nil {
		return nil
	}

	switch col.ColumnType {
	case sqlite3.ColumnTypeNull:
		return nil
	case sqlite3.ColumnTypeInteger:
		return col.Int64()
	case sqlite3.ColumnTypeFloat:
		return col.Float64()
	case sqlite3.ColumnTypeText:
		s := string(col.Text())

		t, err := time.Parse(time.RFC3339Nano, s)
		if err == nil {
			// If parsing is successful, return time.Time directly.
			// The database/sql package will then easily scan this into a time.Time destination.
			return t
		}

		return s
	case sqlite3.ColumnTypeBlob:
		return col.Blob()
	default:
		return nil
	}
}

// RegisterDriver registers the Litebase driver with the sql package
func RegisterDriver(name string, connectionManager *ConnectionManager) {
	globalDriverMutex.Lock()
	defer globalDriverMutex.Unlock()

	// Prevent double registration
	if driverRegistered {
		if registeredDriverName == name {
			// Same driver name, just update the connection manager
			globalDriver = NewLitebaseSQLDriver(connectionManager)
			return
		}

		// Different driver name, this is not allowed
		panic(fmt.Sprintf("litebase driver already registered with name '%s', cannot register with name '%s'", registeredDriverName, name))
	}

	// First time registration
	globalDriver = NewLitebaseSQLDriver(connectionManager)
	sql.Register(name, &GlobalDriverWrapper{})
	driverRegistered = true
	registeredDriverName = name
}
