package nats

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"

	"mysql-cdc/internal/models"
)

// Publisher handles publishing events to NATS
type Publisher struct {
	conn    *nats.Conn
	subject string
	logger  *logrus.Logger
}

// NewPublisher creates a new NATS publisher
func NewPublisher(url, subject string, maxReconnect int, reconnectWait time.Duration, logger *logrus.Logger) (*Publisher, error) {
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

	return &Publisher{
		conn:    conn,
		subject: subject,
		logger:  logger,
	}, nil
}

// Publish publishes a change event to NATS
func (p *Publisher) Publish(event *models.ChangeEvent) error {
	// Use raw JSON if available (from JavaScript transformation), otherwise marshal the struct
	var data []byte
	var err error
	
	if len(event.RawJSON) > 0 {
		data = event.RawJSON
	} else {
		data, err = json.Marshal(event)
		if err != nil {
			return fmt.Errorf("failed to marshal event: %w", err)
		}
	}

	if err := p.conn.Publish(p.subject, data); err != nil {
		return fmt.Errorf("failed to publish to NATS: %w", err)
	}

	p.logger.Debugf("Published %s event for %s.%s", event.Type, event.Database, event.Table)
	return nil
}

// Close closes the NATS connection
func (p *Publisher) Close() {
	if p.conn != nil {
		p.conn.Close()
	}
}

// GetConn returns the underlying NATS connection
func (p *Publisher) GetConn() *nats.Conn {
	return p.conn
}

