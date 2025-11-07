package models

// ChangeEvent represents a database change event
type ChangeEvent struct {
	Type      string                 `json:"type"`      // INSERT, UPDATE, DELETE
	Database  string                 `json:"database"`
	Table     string                 `json:"table"`
	Timestamp int64                  `json:"timestamp"`
	Rows      []map[string]interface{} `json:"rows"`
	OldRows   []map[string]interface{} `json:"old_rows,omitempty"` // For UPDATE events
}

