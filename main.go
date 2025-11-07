package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/sirupsen/logrus"
)

type EventProcessor struct {
	reader   *BinlogReader
	publisher *NATSPublisher
	logger   *logrus.Logger
}

func NewEventProcessor(reader *BinlogReader, publisher *NATSPublisher, logger *logrus.Logger) *EventProcessor {
	return &EventProcessor{
		reader:    reader,
		publisher: publisher,
		logger:    logger,
	}
}

func (p *EventProcessor) ProcessRowEvent(event *replication.RowsEvent) (*ChangeEvent, error) {
	changeEvent := &ChangeEvent{
		Database:  string(event.Table.Schema),
		Table:     string(event.Table.Table),
		Timestamp: time.Now().Unix(),
		Rows:      make([]map[string]interface{}, 0),
		OldRows:   make([]map[string]interface{}, 0),
	}

	// Handle different event types
	if event.Action == replication.UpdateAction {
		changeEvent.Type = "UPDATE"
		// For UPDATE, event.Rows contains [old_row_1, new_row_1, old_row_2, new_row_2, ...]
		for i := 0; i < len(event.Rows); i += 2 {
			if i+1 < len(event.Rows) {
				// Old row
				oldRowMap := make(map[string]interface{})
				for j, col := range event.Table.Columns {
					if j < len(event.Rows[i]) {
						oldRowMap[string(col.Name)] = event.Rows[i][j]
					}
				}
				changeEvent.OldRows = append(changeEvent.OldRows, oldRowMap)

				// New row
				newRowMap := make(map[string]interface{})
				for j, col := range event.Table.Columns {
					if j < len(event.Rows[i+1]) {
						newRowMap[string(col.Name)] = event.Rows[i+1][j]
					}
				}
				changeEvent.Rows = append(changeEvent.Rows, newRowMap)
			}
		}
	} else if event.Action == replication.DeleteAction {
		changeEvent.Type = "DELETE"
		// For DELETE, all rows are the deleted rows
		for _, row := range event.Rows {
			rowMap := make(map[string]interface{})
			for i, col := range event.Table.Columns {
				if i < len(row) {
					rowMap[string(col.Name)] = row[i]
				}
			}
			changeEvent.Rows = append(changeEvent.Rows, rowMap)
		}
	} else if event.Action == replication.InsertAction {
		changeEvent.Type = "INSERT"
		// For INSERT, all rows are the new rows
		for _, row := range event.Rows {
			rowMap := make(map[string]interface{})
			for i, col := range event.Table.Columns {
				if i < len(row) {
					rowMap[string(col.Name)] = row[i]
				}
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
				p.logger.Errorf("Error reading binlog event: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			// Process row events
			switch e := event.Event.(type) {
			case *replication.RowsEvent:
				changeEvent, err := p.ProcessRowEvent(e)
				if err != nil {
					p.logger.Errorf("Error processing row event: %v", err)
					continue
				}

				if err := p.publisher.Publish(changeEvent); err != nil {
					p.logger.Errorf("Error publishing event: %v", err)
					continue
				}

				p.logger.Infof("Processed %s event for %s.%s (%d rows)",
					changeEvent.Type, changeEvent.Database, changeEvent.Table, len(changeEvent.Rows))

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
	processor := NewEventProcessor(reader, publisher, logger)

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

