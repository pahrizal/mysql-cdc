package binlog

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/sirupsen/logrus"
)

// Reader handles reading binlog events from MySQL
type Reader struct {
	syncer       *replication.BinlogSyncer
	streamer     *replication.BinlogStreamer
	position     mysql.Position
	positionFile string
	currentFile  string
	logger       *logrus.Logger
}

// NewReader creates a new binlog reader
func NewReader(host string, port int, user, password string, serverID uint32, flavor string, useGTID bool, positionFile string, startPos uint32, logger *logrus.Logger) (*Reader, error) {
	// Set default flavor if not specified
	if flavor == "" {
		flavor = "mysql"
	}

	cfg := replication.BinlogSyncerConfig{
		ServerID: serverID,
		Flavor:   flavor,
		Host:     host,
		Port:     uint16(port),
		User:     user,
		Password: password,
	}

	// Note: GTID support in go-mysql is handled automatically when using GTID position
	// For now, we use file:position format. GTID can be enabled by using mysql.GTIDSet
	// in the position instead of mysql.Position
	if useGTID {
		logger.Info("GTID replication requested (currently using file:position format)")
	}

	syncer := replication.NewBinlogSyncer(cfg)

	// Load position from file if exists
	position := mysql.Position{
		Name: "",
		Pos:  startPos,
	}

	if data, err := os.ReadFile(positionFile); err == nil {
		if len(data) > 0 {
			posStr := string(data)
			// Parse "filename:position" format
			// Find last colon to handle filenames that might contain colons
			lastColon := -1
			for i := len(posStr) - 1; i >= 0; i-- {
				if posStr[i] == ':' {
					lastColon = i
					break
				}
			}
			if lastColon > 0 && lastColon < len(posStr)-1 {
				name := posStr[:lastColon]
				var pos uint32
				if _, err := fmt.Sscanf(posStr[lastColon+1:], "%d", &pos); err == nil {
					position.Name = name
					position.Pos = pos
					logger.Infof("Loaded binlog position from file: %s:%d", position.Name, position.Pos)
				} else {
					// Fallback to old format (just filename)
					position.Name = posStr
					logger.Infof("Loaded binlog position from file: %s", position.Name)
				}
			} else {
				// Fallback to old format (just filename)
				position.Name = posStr
				logger.Infof("Loaded binlog position from file: %s", position.Name)
			}
		}
	}

	streamer, err := syncer.StartSync(position)
	if err != nil {
		return nil, fmt.Errorf("failed to start binlog sync: %w", err)
	}

	logger.Infof("Started binlog sync from position: %s:%d", position.Name, position.Pos)

	return &Reader{
		syncer:       syncer,
		streamer:     streamer,
		position:     position,
		positionFile: positionFile,
		currentFile:  position.Name,
		logger:       logger,
	}, nil
}

// SavePosition saves the current binlog position to file
func (r *Reader) SavePosition(name string, pos uint32) error {
	if name == "" {
		name = r.currentFile
	}
	if name == "" {
		return nil
	}
	// Save as "filename:position"
	posStr := fmt.Sprintf("%s:%d", name, pos)
	if err := os.WriteFile(r.positionFile, []byte(posStr), 0644); err != nil {
		return fmt.Errorf("failed to save position: %w", err)
	}
	r.position.Name = name
	r.position.Pos = pos
	r.currentFile = name
	return nil
}

// ReadEvent reads the next binlog event
func (r *Reader) ReadEvent() (*replication.BinlogEvent, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	event, err := r.streamer.GetEvent(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get binlog event: %w", err)
	}

	// Handle RotateEvent to update current file name
	if e, ok := event.Event.(*replication.RotateEvent); ok {
		r.currentFile = string(e.NextLogName)
		r.position.Name = r.currentFile
		r.position.Pos = uint32(e.Position)
		if err := r.SavePosition(r.currentFile, r.position.Pos); err != nil {
			r.logger.Warnf("Failed to save position: %v", err)
		}
	} else {
		// Save position after each event
		if event.Header.LogPos > 0 {
			if err := r.SavePosition(r.currentFile, event.Header.LogPos); err != nil {
				r.logger.Warnf("Failed to save position: %v", err)
			}
		}
	}

	return event, nil
}

// Close closes the binlog reader
func (r *Reader) Close() {
	if r.syncer != nil {
		r.syncer.Close()
	}
}

