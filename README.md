# MySQL Change Data Capture (CDC)

A Go-based MySQL Change Data Capture system that reads row-level changes from MySQL binlog and streams them to NATS messaging system.

## Features

- **Row-level CDC**: Captures INSERT, UPDATE, and DELETE operations at the row level
- **Binlog Reading**: Directly reads from MySQL binary log (binlog)
- **NATS Streaming**: Publishes change events to NATS subjects
- **Position Tracking**: Persists binlog position for recovery and resumption
- **Graceful Shutdown**: Handles SIGINT/SIGTERM signals gracefully
- **Configurable**: YAML-based configuration

## Limitations

**Current Limitations:**
- **Output Destination**: Currently only supports streaming to NATS. Other messaging systems (Kafka, RabbitMQ, etc.) are not supported.
- **Source Database**: Only MySQL is supported. MariaDB may work but is not officially tested.
- **MySQL Versions**: Tested with MySQL 5.6, 5.7, and 8.0. Other versions may work but are not guaranteed.

## Prerequisites

- Go 1.21 or higher
- MySQL 5.6+ with binlog enabled (tested with MySQL 5.6, 5.7, 8.0)
- NATS server running

## MySQL Setup

Enable binary logging in MySQL (`my.cnf` or `my.ini`):

**For MySQL 5.6:**
```ini
[mysqld]
# Required for binlog and GTID
log-bin=mysql-bin
binlog-format=ROW
server-id=1

# Required for GTID (MySQL 5.6+)
log-slave-updates=ON
gtid-mode=ON
enforce-gtid-consistency=ON
```

**Important Notes for MySQL 5.6 GTID:**
- `log-bin` must be enabled (required for binary logging)
- `log-slave-updates` must be enabled (required for GTID mode)
- `gtid-mode=ON` enables GTID replication
- `enforce-gtid-consistency=ON` ensures only GTID-safe statements are executed
- After enabling GTID, restart MySQL server

**For MySQL 5.7+:**
```ini
[mysqld]
log-bin=mysql-bin
binlog-format=ROW
server-id=1
```

**Note:** MySQL 5.6 introduced GTID (Global Transaction Identifier) support. You can enable GTID-based replication by setting `use_gtid: true` in the configuration file. This provides better replication reliability and failover capabilities.

Create a MySQL user with replication privileges:

```sql
CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'your_password';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
GRANT SELECT ON *.* TO 'cdc_user'@'%';
FLUSH PRIVILEGES;
```

## Installation

1. Clone or download this repository
2. Install dependencies:

```bash
go mod download
```

3. Build the application:

**Standard build:**
```bash
go build -o mysql-cdc
```

**Static binary for Alpine Linux:**
```bash
# Option 1: Use the build script
./build-static.sh

# Option 2: Build manually
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -a -ldflags '-extldflags "-static"' -o mysql-cdc-linux-amd64
```

The static binary (`mysql-cdc-linux-amd64`) can be run on Alpine Linux without requiring any additional dependencies or glibc.

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
```

### Configuration Options

- **mysql.host**: MySQL server hostname
- **mysql.port**: MySQL server port
- **mysql.user**: MySQL username with replication privileges
- **mysql.password**: MySQL password
- **mysql.server_id**: Unique server ID for replication (must be different from MySQL server)
- **mysql.flavor**: Database flavor (`mysql` or `mariadb`). Defaults to `mysql`
- **mysql.version**: Optional MySQL version string (e.g., "5.6", "5.7", "8.0") for documentation/logging purposes
- **mysql.use_gtid**: Enable GTID-based replication (MySQL 5.6+). Recommended for better reliability and failover
- **binlog.position_file**: File to persist binlog position
- **binlog.start_position**: Starting position (use 4 for beginning)
- **nats.url**: NATS server URL
- **nats.subject**: NATS subject to publish events
- **logging.level**: Log level (debug, info, warn, error)

### MySQL 5.6 Specific Notes

- MySQL 5.6 is fully supported with the `mysql` flavor
- GTID support is available in MySQL 5.6.5+ and can be enabled by setting `use_gtid: true`
- **Required MySQL configuration for GTID:**
  - `log-bin=mysql-bin` (binary logging)
  - `log-slave-updates=ON` (required for GTID mode)
  - `gtid-mode=ON` (enables GTID)
  - `enforce-gtid-consistency=ON` (ensures GTID-safe operations)
  - `binlog-format=ROW` (required for row-level CDC)
- When using GTID, the position file will store GTID information instead of file:position format
- After enabling GTID in MySQL config, restart the MySQL server
- If you get errors about missing `log-bin` or `log-slave-updates`, ensure both are enabled in your MySQL configuration

## Usage

Run the application:

```bash
./mysql-cdc
```

Or specify a custom config file:

```bash
./mysql-cdc /path/to/config.yaml
```

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
      "column2": "value2"
    }
  ],
  "old_rows": [
    {
      "column1": "old_value1",
      "column2": "old_value2"
    }
  ]
}
```

- **INSERT**: Only `rows` field contains the new rows
- **UPDATE**: `rows` contains new values, `old_rows` contains old values
- **DELETE**: Only `rows` field contains the deleted rows

## Testing with NATS

Subscribe to events using NATS CLI:

```bash
nats sub mysql.cdc.events
```

Or using Go:

```go
nc, _ := nats.Connect("nats://localhost:4222")
sub, _ := nc.Subscribe("mysql.cdc.events", func(msg *nats.Msg) {
    fmt.Printf("Received: %s\n", string(msg.Data))
})
```

## Docker / Alpine Linux Support

This project can be built as a static binary that runs on Alpine Linux and other minimal Linux distributions. The static binary includes all dependencies and doesn't require glibc.

### Building Static Binary

Use the provided build script:
```bash
./build-static.sh
```

Or build manually:
```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -a -ldflags '-extldflags "-static"' -o mysql-cdc-linux-amd64
```

### Docker Example

Example Dockerfile using Alpine Linux:

```dockerfile
FROM alpine:latest

# Copy the static binary
COPY mysql-cdc-linux-amd64 /usr/local/bin/mysql-cdc
COPY config.yaml /etc/mysql-cdc/config.yaml

# Make it executable
RUN chmod +x /usr/local/bin/mysql-cdc

# Run the application
CMD ["/usr/local/bin/mysql-cdc", "/etc/mysql-cdc/config.yaml"]
```

## Position Tracking

The application saves the current binlog position to `.binlog_position` file. On restart, it resumes from the last saved position. To start from the beginning, delete this file or set `start_position: 4` in the config.

## Troubleshooting

1. **Connection errors**: Verify MySQL is accessible and user has correct privileges
2. **No events**: Ensure binlog is enabled and `binlog-format=ROW` is set
3. **NATS connection issues**: Verify NATS server is running and URL is correct
4. **Permission errors**: Ensure the application has write access to the position file directory
5. **GTID configuration errors**: 
   - If you get `--gtid-mode=ON requires --log-bin and --log-slave-updates`, ensure both are enabled in MySQL config
   - Add `log-slave-updates=ON` to your `my.cnf` file
   - Restart MySQL after configuration changes
   - See `GTID_CONFIG.md` for detailed GTID setup instructions

## License

MIT

