package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"

	"mysql-cdc/internal/binlog"
	"mysql-cdc/internal/config"
	"mysql-cdc/internal/mysql"
	"mysql-cdc/internal/nats"
	"mysql-cdc/internal/processor"
)

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

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		logger.Fatalf("Failed to load config: %v", err)
	}

	// Set log level from config
	if level, err := logrus.ParseLevel(cfg.Logging.Level); err == nil {
		logger.SetLevel(level)
	}

	logger.Info("Starting MySQL CDC service...")

	// Log MySQL version if specified
	if cfg.MySQL.Version != "" {
		logger.Infof("MySQL version: %s", cfg.MySQL.Version)
	}
	if cfg.MySQL.UseGTID {
		logger.Info("GTID replication will be used")
	}

	// Verify MySQL connection and permissions before starting binlog sync
	logger.Info("Verifying MySQL connection and permissions...")
	checker := mysql.NewChecker(
		cfg.MySQL.Host,
		cfg.MySQL.Port,
		cfg.MySQL.User,
		cfg.MySQL.Password,
		logger,
	)
	if err := checker.CheckConnectionAndPermissions(); err != nil {
		logger.Fatalf("MySQL connection/permission check failed: %v", err)
	}

	// Initialize binlog reader
	reader, err := binlog.NewReader(
		cfg.MySQL.Host,
		cfg.MySQL.Port,
		cfg.MySQL.User,
		cfg.MySQL.Password,
		cfg.MySQL.ServerID,
		cfg.MySQL.Flavor,
		cfg.MySQL.UseGTID,
		cfg.Binlog.PositionFile,
		cfg.Binlog.StartPosition,
		logger,
	)
	if err != nil {
		logger.Fatalf("Failed to create binlog reader: %v", err)
	}
	defer reader.Close()

	// Validate processor configuration
	if err := processor.ValidateRules(&cfg.Processor); err != nil {
		logger.Fatalf("Invalid processor configuration: %v", err)
	}

	// Initialize NATS publisher first (needed for transformer)
	publisher, err := nats.NewPublisher(
		cfg.NATS.URL,
		cfg.NATS.Subject,
		cfg.NATS.MaxReconnect,
		cfg.NATS.ReconnectWait,
		logger,
	)
	if err != nil {
		logger.Fatalf("Failed to create NATS publisher: %v", err)
	}
	defer publisher.Close()

	// Initialize transformer with NATS connection
	transformer, err := processor.NewTransformer(&cfg.Processor, logger, publisher.GetConn())
	if err != nil {
		logger.Fatalf("Failed to create transformer: %v", err)
	}
	if cfg.Processor.Enabled {
		if cfg.Processor.Script != "" {
			logger.Info("Processor/transformer enabled with JavaScript script")
		} else if len(cfg.Processor.Rules) > 0 {
			logger.Info("Processor/transformer enabled with YAML rules")
		}
	}

	// Create event processor
	proc, err := processor.NewProcessor(
		reader,
		publisher,
		transformer,
		cfg.MySQL.Host,
		cfg.MySQL.Port,
		cfg.MySQL.User,
		cfg.MySQL.Password,
		logger,
	)
	if err != nil {
		logger.Fatalf("Failed to create event processor: %v", err)
	}
	defer proc.Close()

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start processing in goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- proc.Start(ctx)
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

