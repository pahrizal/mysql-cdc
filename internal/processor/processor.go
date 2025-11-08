package processor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/sirupsen/logrus"

	"mysql-cdc/internal/models"
)

// Processor processes binlog events and publishes them
type Processor struct {
	reader      Reader
	publisher   Publisher
	transformer *Transformer
	logger      *logrus.Logger
	tables       map[uint64]*replication.TableMapEvent // Cache table map events
	columnNames  map[string][]string                    // Cache column names by "database.table"
	columnTypes  map[string][]string                    // Cache column types by "database.table"
	db           *sql.DB                                // Database connection for fetching column names
}

// Reader interface for reading binlog events
type Reader interface {
	ReadEvent() (*replication.BinlogEvent, error)
}

// Publisher interface for publishing events
type Publisher interface {
	Publish(event *models.ChangeEvent) error
}

// NewProcessor creates a new event processor
func NewProcessor(reader Reader, publisher Publisher, transformer *Transformer, dbHost string, dbPort int, dbUser, dbPassword string, logger *logrus.Logger) (*Processor, error) {
	// Create database connection for fetching column names
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/", dbUser, dbPassword, dbHost, dbPort)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	return &Processor{
		reader:      reader,
		publisher:   publisher,
		transformer: transformer,
		logger:      logger,
		tables:      make(map[uint64]*replication.TableMapEvent),
		columnNames: make(map[string][]string),
		columnTypes: make(map[string][]string),
		db:          db,
	}, nil
}

// Close closes the processor and its database connection
func (p *Processor) Close() {
	if p.db != nil {
		p.db.Close()
	}
}

// getColumnInfo fetches column names and types from MySQL for a given table
func (p *Processor) getColumnInfo(database, table string) ([]string, []string, error) {
	// Check cache first
	cacheKey := fmt.Sprintf("%s.%s", database, table)
	if cols, ok := p.columnNames[cacheKey]; ok {
		types, _ := p.columnTypes[cacheKey]
		return cols, types, nil
	}

	// Query INFORMATION_SCHEMA for column names and types
	query := `
		SELECT COLUMN_NAME, COLUMN_TYPE
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? 
		ORDER BY ORDINAL_POSITION
	`
	rows, err := p.db.Query(query, database, table)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query column info: %w", err)
	}
	defer rows.Close()

	var columns []string
	var types []string
	for rows.Next() {
		var colName, columnType string
		if err := rows.Scan(&colName, &columnType); err != nil {
			return nil, nil, fmt.Errorf("failed to scan column info: %w", err)
		}
		columns = append(columns, colName)
		types = append(types, columnType) // Use COLUMN_TYPE for more detailed info
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("error iterating columns: %w", err)
	}

	// Cache the results
	p.columnNames[cacheKey] = columns
	p.columnTypes[cacheKey] = types
	p.logger.Debugf("Fetched %d column names and types for %s.%s", len(columns), database, table)

	return columns, types, nil
}

// ProcessRowEvent processes a row event and returns a change event
func (p *Processor) ProcessRowEvent(event *replication.RowsEvent, eventType string) (*models.ChangeEvent, error) {
	// Get table map for column information
	tableMap, ok := p.tables[event.TableID]
	if !ok {
		return nil, fmt.Errorf("table map not found for table ID %d", event.TableID)
	}

	database := string(event.Table.Schema)
	table := string(event.Table.Table)

	// Get column names and types - try from TableMapEvent first (MySQL 8.0+), otherwise fetch from MySQL
	var columnNames []string
	var columnTypes []string
	if len(tableMap.ColumnName) > 0 {
		// Column names available in binlog (MySQL 8.0+ with binlog_row_metadata)
		columnNames = make([]string, len(tableMap.ColumnName))
		for i, col := range tableMap.ColumnName {
			columnNames[i] = string(col)
		}
		// Still need to fetch types from MySQL for MySQL 8.0+
		var err error
		_, columnTypes, err = p.getColumnInfo(database, table)
		if err != nil {
			p.logger.Warnf("Failed to get column types: %v, continuing without type info", err)
		}
	} else {
		// Fetch column names and types from MySQL (for MySQL 5.6/5.7)
		var err error
		columnNames, columnTypes, err = p.getColumnInfo(database, table)
		if err != nil {
			return nil, fmt.Errorf("failed to get column info: %w", err)
		}
		// Ensure we have enough column names
		if len(columnNames) < int(tableMap.ColumnCount) {
			p.logger.Warnf("Column count mismatch: expected %d columns, got %d names", tableMap.ColumnCount, len(columnNames))
		}
	}

	changeEvent := &models.ChangeEvent{
		Database:  database,
		Table:     table,
		Timestamp: time.Now().Unix(),
		Rows:      make([]map[string]interface{}, 0),
		OldRows:   make([]map[string]interface{}, 0),
		Type:      eventType,
	}

	// Helper function to convert value based on column type
	convertValue := func(value interface{}, colIndex int) interface{} {
		if value == nil {
			return nil
		}
		
		// If we have column type info, check if it's a TEXT type
		if colIndex < len(columnTypes) {
			colType := strings.ToUpper(columnTypes[colIndex])
			// Check if it's a TEXT type (TEXT, TINYTEXT, MEDIUMTEXT, LONGTEXT)
			if strings.Contains(colType, "TEXT") {
				// Convert []byte to string for TEXT columns
				if b, ok := value.([]byte); ok {
					return string(b)
				}
			}
			// For BLOB types, keep as base64 (or could convert to string if desired)
			// BLOB types are kept as []byte which will be base64 encoded in JSON
		}
		
		// For []byte values without type info, try to convert to string
		// This handles cases where type info is not available
		if b, ok := value.([]byte); ok {
			// Try to detect if it's valid UTF-8 text
			if len(b) > 0 {
				// Check if it looks like text (not binary)
				// Simple heuristic: if it's valid UTF-8 and doesn't contain null bytes, treat as text
				if len(b) < 65535 { // Reasonable size limit for TEXT
					if str := string(b); len(str) == len(b) {
						// No null bytes, likely text
						return str
					}
				}
			}
		}
		
		return value
	}

	// Process rows based on event type
	if eventType == "UPDATE" {
		// For UPDATE, event.Rows contains [old_row_1, new_row_1, old_row_2, new_row_2, ...]
		for i := 0; i < len(event.Rows); i += 2 {
			if i+1 < len(event.Rows) {
				// Old row
				oldRowMap := make(map[string]interface{})
				for j := 0; j < len(event.Rows[i]) && j < len(columnNames); j++ {
					oldRowMap[columnNames[j]] = convertValue(event.Rows[i][j], j)
				}
				changeEvent.OldRows = append(changeEvent.OldRows, oldRowMap)

				// New row
				newRowMap := make(map[string]interface{})
				for j := 0; j < len(event.Rows[i+1]) && j < len(columnNames); j++ {
					newRowMap[columnNames[j]] = convertValue(event.Rows[i+1][j], j)
				}
				changeEvent.Rows = append(changeEvent.Rows, newRowMap)
			}
		}
	} else {
		// For INSERT and DELETE, all rows are the affected rows
		for _, row := range event.Rows {
			rowMap := make(map[string]interface{})
			for j := 0; j < len(row) && j < len(columnNames); j++ {
				rowMap[columnNames[j]] = convertValue(row[j], j)
			}
			changeEvent.Rows = append(changeEvent.Rows, rowMap)
		}
	}

	return changeEvent, nil
}

// Start starts processing binlog events
func (p *Processor) Start(ctx context.Context) error {
	p.logger.Info("Starting event processor...")

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("Context cancelled, stopping event processor")
			return nil
		default:
			event, err := p.reader.ReadEvent()
			if err != nil {
				// Check if it's a timeout error (context deadline exceeded)
				// This is normal when there are no events, so we don't log it as an error
				if errors.Is(err, context.DeadlineExceeded) ||
					strings.Contains(err.Error(), "context deadline exceeded") {
					// Timeout is expected when waiting for events, just continue
					continue
				}
				// Log other errors as they indicate real problems
				p.logger.Errorf("Error reading binlog event: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			// Process row events
			switch e := event.Event.(type) {
			case *replication.TableMapEvent:
				// Cache table map events for column information
				p.tables[e.TableID] = e
				p.logger.Debugf("Cached table map for %s.%s (ID: %d)", string(e.Schema), string(e.Table), e.TableID)

			case *replication.RowsEvent:
				// Determine event type from header
				var eventType string
				switch event.Header.EventType {
				case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
					eventType = "INSERT"
				case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
					eventType = "UPDATE"
				case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
					eventType = "DELETE"
				default:
					p.logger.Debugf("Unhandled row event type: %d", event.Header.EventType)
					continue
				}

				changeEvent, err := p.ProcessRowEvent(e, eventType)
				if err != nil {
					p.logger.Errorf("Error processing %s event: %v", eventType, err)
					continue
				}

				// Store database/table info before transformation (in case event is rejected)
				database := changeEvent.Database
				table := changeEvent.Table

				// Apply transformations if transformer is configured
				if p.transformer != nil {
					changeEvent, err = p.transformer.Transform(changeEvent)
					if err != nil {
						// Check if event was rejected (not an error, just skip publishing)
						if errors.Is(err, ErrEventRejected) {
							p.logger.Debugf("Event rejected by transformer: %s.%s (type: %s)", database, table, eventType)
							continue
						}
						p.logger.Errorf("Error transforming event: %v", err)
						continue
					}
					// Check if changeEvent became nil after transformation
					if changeEvent == nil {
						p.logger.Debugf("Event rejected by transformer: %s.%s (type: %s)", database, table, eventType)
						continue
					}
				}

				if err := p.publisher.Publish(changeEvent); err != nil {
					p.logger.Errorf("Error publishing event: %v", err)
					continue
				}
				p.logger.Infof("Processed %s event for %s.%s (%d rows)",
					eventType, changeEvent.Database, changeEvent.Table, len(changeEvent.Rows))

			case *replication.RotateEvent:
				p.logger.Infof("Binlog rotated to: %s", string(e.NextLogName))
				// Position is already saved in ReadEvent

			case *replication.QueryEvent:
				p.logger.Debugf("Query event: %s", string(e.Query))

			case *replication.XIDEvent:
				p.logger.Debugf("XID event: %d", e.XID)

			default:
				p.logger.Debugf("Unhandled event type: %T", e)
			}
		}
	}
}

