package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	MySQL   MySQLConfig   `yaml:"mysql"`
	Binlog  BinlogConfig  `yaml:"binlog"`
	NATS    NATSConfig    `yaml:"nats"`
	Logging LoggingConfig `yaml:"logging"`
}

type MySQLConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	ServerID uint32 `yaml:"server_id"`
	Flavor   string `yaml:"flavor"` // mysql, mariadb
	Version  string `yaml:"version"` // Optional: 5.6, 5.7, 8.0, etc.
	UseGTID  bool   `yaml:"use_gtid"` // Use GTID for replication (MySQL 5.6+)
}

type BinlogConfig struct {
	PositionFile  string `yaml:"position_file"`
	StartPosition uint32 `yaml:"start_position"`
	StartTimestamp uint32 `yaml:"start_timestamp"`
}

type NATSConfig struct {
	URL           string        `yaml:"url"`
	Subject       string        `yaml:"subject"`
	MaxReconnect  int           `yaml:"max_reconnect"`
	ReconnectWait time.Duration `yaml:"reconnect_wait"`
}

type LoggingConfig struct {
	Level string `yaml:"level"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Set defaults
	if config.NATS.ReconnectWait == 0 {
		config.NATS.ReconnectWait = 2 * time.Second
	}
	if config.MySQL.Flavor == "" {
		config.MySQL.Flavor = "mysql"
	}

	return &config, nil
}

