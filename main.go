package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/sirupsen/logrus"
)

type EventProcessor struct {
	reader       *BinlogReader
	publisher    *NATSPublisher
	logger       *logrus.Logger
	tables       map[uint64]*replication.TableMapEvent // Cache table map events
	columnNames  map[string][]string                    // Cache column names by "database.table"
	db           *sql.DB                                // Database connection for fetching column names
}

func NewEventProcessor(reader *BinlogReader, publisher *NATSPublisher, dbHost string, dbPort int, dbUser, dbPassword string, logger *logrus.Logger) (*EventProcessor, error) {
	// Create database connection for fetching column names
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/", dbUser, dbPassword, dbHost, dbPort)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	return &EventProcessor{
		reader:      reader,
		publisher:   publisher,
		logger:      logger,
		tables:      make(map[uint64]*replication.TableMapEvent),
		columnNames: make(map[string][]string),
		db:          db,
	}, nil
}

func (p *EventProcessor) Close() {
	if p.db != nil {
		p.db.Close()
	}
}

// getColumnNames fetches column names from MySQL for a given table
func (p *EventProcessor) getColumnNames(database, table string) ([]string, error) {
	// Check cache first
	cacheKey := fmt.Sprintf("%s.%s", database, table)
	if cols, ok := p.columnNames[cacheKey]; ok {
		return cols, nil
	}

	// Query INFORMATION_SCHEMA for column names
	query := `
		SELECT COLUMN_NAME 
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? 
		ORDER BY ORDINAL_POSITION
	`
	rows, err := p.db.Query(query, database, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query column names: %w", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return nil, fmt.Errorf("failed to scan column name: %w", err)
		}
		columns = append(columns, colName)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating columns: %w", err)
	}

	// Cache the result
	p.columnNames[cacheKey] = columns
	p.logger.Debugf("Fetched %d column names for %s.%s", len(columns), database, table)

	return columns, nil
}

func (p *EventProcessor) ProcessRowEvent(event *replication.RowsEvent, eventType string) (*ChangeEvent, error) {
	// Get table map for column information
	tableMap, ok := p.tables[event.TableID]
	if !ok {
		return nil, fmt.Errorf("table map not found for table ID %d", event.TableID)
	}

	database := string(event.Table.Schema)
	table := string(event.Table.Table)

	// Get column names - try from TableMapEvent first (MySQL 8.0+), otherwise fetch from MySQL
	var columnNames []string
	if len(tableMap.ColumnName) > 0 {
		// Column names available in binlog (MySQL 8.0+ with binlog_row_metadata)
		columnNames = make([]string, len(tableMap.ColumnName))
		for i, col := range tableMap.ColumnName {
			columnNames[i] = string(col)
		}
	} else {
		// Fetch column names from MySQL (for MySQL 5.6/5.7)
		var err error
		columnNames, err = p.getColumnNames(database, table)
		if err != nil {
			return nil, fmt.Errorf("failed to get column names: %w", err)
		}
		// Ensure we have enough column names
		if len(columnNames) < int(tableMap.ColumnCount) {
			p.logger.Warnf("Column count mismatch: expected %d columns, got %d names", tableMap.ColumnCount, len(columnNames))
		}
	}

	changeEvent := &ChangeEvent{
		Database:  database,
		Table:     table,
		Timestamp: time.Now().Unix(),
		Rows:      make([]map[string]interface{}, 0),
		OldRows:   make([]map[string]interface{}, 0),
		Type:      eventType,
	}

	// Process rows based on event type
	if eventType == "UPDATE" {
		// For UPDATE, event.Rows contains [old_row_1, new_row_1, old_row_2, new_row_2, ...]
		for i := 0; i < len(event.Rows); i += 2 {
			if i+1 < len(event.Rows) {
				// Old row
				oldRowMap := make(map[string]interface{})
				for j := 0; j < len(event.Rows[i]) && j < len(columnNames); j++ {
					oldRowMap[columnNames[j]] = event.Rows[i][j]
				}
				changeEvent.OldRows = append(changeEvent.OldRows, oldRowMap)

				// New row
				newRowMap := make(map[string]interface{})
				for j := 0; j < len(event.Rows[i+1]) && j < len(columnNames); j++ {
					newRowMap[columnNames[j]] = event.Rows[i+1][j]
				}
				changeEvent.Rows = append(changeEvent.Rows, newRowMap)
			}
		}
	} else {
		// For INSERT and DELETE, all rows are the affected rows
		for _, row := range event.Rows {
			rowMap := make(map[string]interface{})
			for j := 0; j < len(row) && j < len(columnNames); j++ {
				rowMap[columnNames[j]] = row[j]
			}
			changeEvent.Rows = append(changeEvent.Rows, rowMap)
		}
	}

	return changeEvent, nil
}

func (p *EventProcessor) Start(ctx context.Context) error {
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

func main() {
	// Setup logger
	logger := logrus.New()
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
	logger.SetLevel(logrus.InfoLevel)

	// Load configuration
	configPath := "config.yaml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	config, err := LoadConfig(configPath)
	if err != nil {
		logger.Fatalf("Failed to load config: %v", err)
	}

	// Set log level from config
	if level, err := logrus.ParseLevel(config.Logging.Level); err == nil {
		logger.SetLevel(level)
	}

	logger.Info("Starting MySQL CDC service...")

	// Log MySQL version if specified
	if config.MySQL.Version != "" {
		logger.Infof("MySQL version: %s", config.MySQL.Version)
	}
	if config.MySQL.UseGTID {
		logger.Info("GTID replication will be used")
	}

	// Verify MySQL connection and permissions before starting binlog sync
	logger.Info("Verifying MySQL connection and permissions...")
	checker := NewMySQLChecker(
		config.MySQL.Host,
		config.MySQL.Port,
		config.MySQL.User,
		config.MySQL.Password,
		logger,
	)
	if err := checker.CheckConnectionAndPermissions(); err != nil {
		logger.Fatalf("MySQL connection/permission check failed: %v", err)
	}

	// Initialize binlog reader
	reader, err := NewBinlogReader(
		config.MySQL.Host,
		config.MySQL.Port,
		config.MySQL.User,
		config.MySQL.Password,
		config.MySQL.ServerID,
		config.MySQL.Flavor,
		config.MySQL.UseGTID,
		config.Binlog.PositionFile,
		config.Binlog.StartPosition,
		logger,
	)
	if err != nil {
		logger.Fatalf("Failed to create binlog reader: %v", err)
	}
	defer reader.Close()

	// Initialize NATS publisher
	publisher, err := NewNATSPublisher(
		config.NATS.URL,
		config.NATS.Subject,
		config.NATS.MaxReconnect,
		config.NATS.ReconnectWait,
		logger,
	)
	if err != nil {
		logger.Fatalf("Failed to create NATS publisher: %v", err)
	}
	defer publisher.Close()

	// Create event processor
	processor, err := NewEventProcessor(
		reader,
		publisher,
		config.MySQL.Host,
		config.MySQL.Port,
		config.MySQL.User,
		config.MySQL.Password,
		logger,
	)
	if err != nil {
		logger.Fatalf("Failed to create event processor: %v", err)
	}
	defer processor.Close()

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start processing in goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- processor.Start(ctx)
	}()

	// Wait for signal or error
	select {
	case sig := <-sigChan:
		logger.Infof("Received signal: %v, shutting down...", sig)
		cancel()
	case err := <-errChan:
		if err != nil {
			logger.Errorf("Processor error: %v", err)
		}
	}

	logger.Info("MySQL CDC service stopped")
}

