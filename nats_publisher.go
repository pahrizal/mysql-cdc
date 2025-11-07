package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

type NATSPublisher struct {
	conn    *nats.Conn
	subject string
	logger  *logrus.Logger
}

func NewNATSPublisher(url, subject string, maxReconnect int, reconnectWait time.Duration, logger *logrus.Logger) (*NATSPublisher, error) {
	opts := []nats.Option{
		nats.MaxReconnects(maxReconnect),
		nats.ReconnectWait(reconnectWait),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				logger.Warnf("NATS disconnected: %v", err)
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Infof("NATS reconnected to %s", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			logger.Warn("NATS connection closed")
		}),
	}

	conn, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	logger.Infof("Connected to NATS at %s", url)

	return &NATSPublisher{
		conn:    conn,
		subject: subject,
		logger:  logger,
	}, nil
}

type ChangeEvent struct {
	Type      string                 `json:"type"`      // INSERT, UPDATE, DELETE
	Database  string                 `json:"database"`
	Table     string                 `json:"table"`
	Timestamp int64                  `json:"timestamp"`
	Rows      []map[string]interface{} `json:"rows"`
	OldRows   []map[string]interface{} `json:"old_rows,omitempty"` // For UPDATE events
}

func (p *NATSPublisher) Publish(event *ChangeEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	if err := p.conn.Publish(p.subject, data); err != nil {
		return fmt.Errorf("failed to publish to NATS: %w", err)
	}

	p.logger.Debugf("Published %s event for %s.%s", event.Type, event.Database, event.Table)
	return nil
}

func (p *NATSPublisher) Close() {
	if p.conn != nil {
		p.conn.Close()
	}
}

