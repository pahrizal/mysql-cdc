# MySQL Change Data Capture (CDC)

A Go-based MySQL Change Data Capture system that reads row-level changes from MySQL binlog and streams them to NATS messaging system.

## Features

- **Row-level CDC**: Captures INSERT, UPDATE, and DELETE operations at the row level
- **Binlog Reading**: Directly reads from MySQL binary log (binlog)
- **NATS Streaming**: Publishes change events to NATS subjects
- **Data Transformation**: Configurable processor to transform data before publishing (YAML rules or JavaScript scripts)
- **Position Tracking**: Persists binlog position for recovery and resumption
- **Graceful Shutdown**: Handles SIGINT/SIGTERM signals gracefully
- **Configurable**: YAML-based configuration

## MySQL Setup

Enable binary logging in MySQL (`my.cnf` or `my.ini`):

**Basic Configuration:**
```ini
[mysqld]
log-bin=mysql-bin
binlog-format=ROW
server-id=1
```

**For GTID Support (MySQL 5.6+):**
```ini
[mysqld]
log-bin=mysql-bin
binlog-format=ROW
server-id=1
log-slave-updates=ON
gtid-mode=ON
enforce-gtid-consistency=ON
```

Create a MySQL user with replication privileges:

```sql
CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'your_password';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
GRANT SELECT ON *.* TO 'cdc_user'@'%';
FLUSH PRIVILEGES;
```

## Installation

```bash
go mod download
go build -o mysql-cdc ./cmd/mysql-cdc
```

## Configuration

Edit `config.yaml` to match your environment:

```yaml
mysql:
  host: localhost
  port: 3306
  user: cdc_user
  password: your_password
  server_id: 1001
  flavor: mysql  # mysql or mariadb
  version: "5.6"  # Optional: 5.6, 5.7, 8.0, etc. (for documentation/logging)
  use_gtid: false  # Enable GTID replication (MySQL 5.6+)

binlog:
  position_file: .binlog_position
  start_position: 4
  start_timestamp: 0

nats:
  url: nats://localhost:4222
  subject: mysql.cdc.events
  max_reconnect: 10
  reconnect_wait: 2s

logging:
  level: info

processor:
  enabled: false  # Set to true to enable data transformation
  # script: scripts/transform.js  # Path to JavaScript transformation script (takes precedence over rules)
  rules:
    # YAML-based transformation rules (see Processor Configuration section)
```

### Configuration Options

- **mysql.host**: MySQL server hostname
- **mysql.port**: MySQL server port
- **mysql.user**: MySQL username with replication privileges
- **mysql.password**: MySQL password
- **mysql.server_id**: Unique server ID for replication (must be different from MySQL server)
- **mysql.flavor**: Database flavor (`mysql` or `mariadb`). Defaults to `mysql`
- **mysql.use_gtid**: Enable GTID-based replication (MySQL 5.6+)
- **binlog.position_file**: File to persist binlog position
- **binlog.start_position**: Starting position (use 4 for beginning)
- **nats.url**: NATS server URL
- **nats.subject**: NATS subject to publish events
- **logging.level**: Log level (debug, info, warn, error)
- **processor.enabled**: Enable/disable data transformation
- **processor.script**: Path to JavaScript transformation script (takes precedence over rules)
- **processor.rules**: YAML-based transformation rules

## Usage

Run the application:

```bash
./mysql-cdc
```

Or specify a custom config file:

```bash
./mysql-cdc /path/to/config.yaml
```

## Processor Configuration

The processor allows you to transform change events before they are published to NATS. You can use either JavaScript scripts or YAML-based rules.

### JavaScript Script Processor

You can write custom JavaScript transformation scripts for maximum flexibility. The script can use either:
1. **Anonymous function** (recommended): `(function(event) { return event; })`
2. **Named function** (backward compatible): `function transform(event) { return event; }`

**Key Features:**
- **Event Rejection**: Return `null` or `undefined` to reject/drop an event (it won't be published to NATS)
- **Full Event Access**: Access all event properties including `type`, `database`, `table`, `timestamp`, `rows`, and `old_rows`
- **Row Transformation**: Modify, filter, or add fields to individual rows
- **Metadata Addition**: Add custom fields to the event object

**Example JavaScript script using anonymous function (`scripts/transform.js`):**

```javascript
(function(event) {
    // Example: Reject events from certain tables
    if (event.table === 'sensitive_table') {
        return null; // Reject/drop this event
    }
    
    // Example: Reject DELETE events
    if (event.type === 'DELETE') {
        return null;
    }
    
    // Example: Reject events based on row data
    if (event.rows && event.rows.length > 0) {
        var firstRow = event.rows[0];
        if (firstRow.status === 'deleted' || firstRow.status === 'archived') {
            return null; // Reject events where status is 'deleted' or 'archived'
        }
    }
    
    // Add a processed timestamp
    event.processed_at = new Date().toISOString();
    
    // Transform rows - exclude sensitive fields
    if (event.rows && Array.isArray(event.rows)) {
        event.rows = event.rows.map(function(row) {
            // Remove password field if it exists
            if (row.password !== undefined) {
                delete row.password;
            }
            // Rename email to user_email
            if (row.email !== undefined) {
                row.user_email = row.email;
                delete row.email;
            }
            return row;
        });
    }
    
    // Add metadata
    event.metadata = {
        source: "mysql-cdc",
        processor: "javascript"
    };
    
    return event;
})

// Alternative: Named function (also supported)
// function transform(event) {
//     // ... same code ...
//     return event;
// }
```

**Configuration:**

```yaml
processor:
  enabled: true
  script: scripts/transform.js
```

**Event Rejection:** Return `null` or `undefined` to reject/drop an event (it won't be published to NATS).

### YAML-Based Rules Processor

For simpler transformations, you can use YAML-based rules:

```yaml
processor:
  enabled: true
  rules:
    # Exclude sensitive fields from a specific table
    - database: mydb
      table: users
      exclude:
        - password
        - ssn
      rename:
        email: user_email
        name: full_name
      add_fields:
        source: mysql-cdc
    
    # Include only specific fields for all tables in a database
    - database: mydb
      include:
        - id
        - name
        - created_at
    
    # Exclude sensitive fields from all tables
    - database: ""
      table: ""
      exclude:
        - password
        - credit_card
```

**Rule Options:**

- **database**: Database name (empty string = all databases)
- **table**: Table name (empty string = all tables)
- **include**: List of fields to include (all other fields excluded)
- **exclude**: List of fields to exclude
- **rename**: Map of old field names to new field names
- **add_fields**: Map of static field names and values to add

**Note:** You cannot specify both `include` and `exclude` in the same rule. If both `script` and `rules` are specified, the script takes precedence.

## Event Format

Events are published to NATS as JSON messages with the following structure:

```json
{
  "type": "INSERT|UPDATE|DELETE",
  "database": "database_name",
  "table": "table_name",
  "timestamp": 1234567890,
  "rows": [
    {
      "column1": "value1",
      "column2": "value2",
      "text_field": "readable text content",
      "json_field": "{\"key\":\"value\"}"
    }
  ],
  "old_rows": [
    {
      "column1": "old_value1",
      "column2": "old_value2"
    }
  ],
  "processed_at": "2024-01-01T12:00:00Z",
  "metadata": {
    "source": "mysql-cdc",
    "processor": "javascript"
  }
}
```

- **INSERT**: Only `rows` field contains the new rows
- **UPDATE**: `rows` contains new values, `old_rows` contains old values
- **DELETE**: Only `rows` field contains the deleted rows

### Data Type Handling

- **TEXT Fields**: Automatically converted from binary/byte arrays to readable strings (TEXT, TINYTEXT, MEDIUMTEXT, LONGTEXT)
- **BLOB Fields**: Kept as base64-encoded strings in JSON (BLOB, TINYBLOB, MEDIUMBLOB, LONGBLOB)
- **Other Types**: Standard MySQL types are preserved as-is (INT, VARCHAR, DATETIME, etc.)

**Note:** The processor automatically detects TEXT column types and converts them to strings, so you'll see readable text content instead of base64-encoded strings for TEXT fields.

## Position Tracking

The application saves the current binlog position to `.binlog_position` file. On restart, it resumes from the last saved position. To start from the beginning, delete this file or set `start_position: 4` in the config.

## Troubleshooting

1. **Connection errors**: Verify MySQL is accessible and user has correct privileges
2. **No events**: Ensure binlog is enabled and `binlog-format=ROW` is set
3. **NATS connection issues**: Verify NATS server is running and URL is correct
4. **Permission errors**: Ensure the application has write access to the position file directory
5. **GTID configuration errors**: Ensure `log-bin` and `log-slave-updates` are enabled in MySQL config
6. **JavaScript processor errors**: Check script syntax and file path, enable debug logging for details
7. **TEXT fields showing as base64**: Ensure MySQL user has SELECT permission on INFORMATION_SCHEMA

## License

MIT

