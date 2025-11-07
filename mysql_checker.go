package main

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
)

// MySQLChecker validates MySQL connection and required permissions
type MySQLChecker struct {
	host     string
	port     int
	user     string
	password string
	logger   *logrus.Logger
}

// NewMySQLChecker creates a new MySQL checker
func NewMySQLChecker(host string, port int, user, password string, logger *logrus.Logger) *MySQLChecker {
	return &MySQLChecker{
		host:     host,
		port:     port,
		user:     user,
		password: password,
		logger:   logger,
	}
}

// CheckConnectionAndPermissions verifies MySQL connection and required permissions
func (c *MySQLChecker) CheckConnectionAndPermissions() error {
	// Build DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/", c.user, c.password, c.host, c.port)

	// Test connection
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open MySQL connection: %w", err)
	}
	defer db.Close()

	// Set connection timeout
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// Test connection with ping
	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to connect to MySQL server: %w", err)
	}

	c.logger.Info("Successfully connected to MySQL server")

	// Check required permissions
	requiredPrivs := []string{
		"REPLICATION SLAVE",
		"REPLICATION CLIENT",
		"SELECT",
	}

	// Get current user grants (SHOW GRANTS can return multiple rows)
	var allGrants strings.Builder
	rows, err := db.Query("SHOW GRANTS FOR CURRENT_USER()")
	if err != nil {
		// Try alternative query for MySQL 5.6
		rows, err = db.Query("SHOW GRANTS")
		if err != nil {
			return fmt.Errorf("failed to check grants: %w", err)
		}
	}
	defer rows.Close()

	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return fmt.Errorf("failed to scan grant: %w", err)
		}
		if allGrants.Len() > 0 {
			allGrants.WriteString("; ")
		}
		allGrants.WriteString(grant)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating grants: %w", err)
	}

	grantsStr := allGrants.String()
	grantsUpper := strings.ToUpper(grantsStr)
	missingPrivs := []string{}

	for _, priv := range requiredPrivs {
		if !strings.Contains(grantsUpper, priv) {
			missingPrivs = append(missingPrivs, priv)
		}
	}

	if len(missingPrivs) > 0 {
		return fmt.Errorf("missing required permissions: %s. Current grants: %s", strings.Join(missingPrivs, ", "), grantsStr)
	}

	c.logger.Info("All required permissions verified")

	// Check if binlog is enabled
	var logBin string
	err = db.QueryRow("SHOW VARIABLES LIKE 'log_bin'").Scan(&logBin, &logBin)
	if err != nil {
		// Try alternative query
		var value string
		err = db.QueryRow("SELECT @@log_bin").Scan(&value)
		if err != nil {
			c.logger.Warn("Could not verify binlog status")
		} else {
			if value == "0" || value == "OFF" {
				return fmt.Errorf("binary logging (log_bin) is not enabled. Enable it in MySQL configuration")
			}
			c.logger.Info("Binary logging is enabled")
		}
	} else {
		if logBin != "ON" && logBin != "1" {
			return fmt.Errorf("binary logging (log_bin) is not enabled. Current value: %s. Enable it in MySQL configuration", logBin)
		}
		c.logger.Info("Binary logging is enabled")
	}

	// Check binlog format (should be ROW for CDC)
	var binlogFormat string
	err = db.QueryRow("SHOW VARIABLES LIKE 'binlog_format'").Scan(&binlogFormat, &binlogFormat)
	if err != nil {
		// Try alternative query
		var value string
		err = db.QueryRow("SELECT @@binlog_format").Scan(&value)
		if err == nil {
			binlogFormat = value
		}
	}

	if binlogFormat != "" && binlogFormat != "ROW" {
		c.logger.Warnf("binlog_format is set to '%s', but ROW format is recommended for CDC", binlogFormat)
	} else if binlogFormat == "ROW" {
		c.logger.Info("binlog_format is set to ROW (recommended for CDC)")
	}

	return nil
}

