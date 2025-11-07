# MySQL 5.6 GTID Configuration Guide

## Error Resolution

If you encounter the error:
```
[ERROR] --gtid-mode=ON or UPGRADE_STEP_1 or UPGRADE_STEP_2 requires --log-bin and --log-slave-updates
```

This means MySQL requires both `log-bin` and `log-slave-updates` to be enabled before GTID can be activated.

## Step-by-Step Configuration

### 1. Edit MySQL Configuration File

Locate your MySQL configuration file:
- Linux: `/etc/mysql/my.cnf` or `/etc/my.cnf`
- macOS (Homebrew): `/usr/local/etc/my.cnf` or `/opt/homebrew/etc/my.cnf`
- Windows: `C:\ProgramData\MySQL\MySQL Server 5.6\my.ini`

### 2. Add Required Settings

Add or update the `[mysqld]` section:

```ini
[mysqld]
# Binary logging (required for CDC and GTID)
log-bin=mysql-bin
binlog-format=ROW
server-id=1

# Required for GTID mode
log-slave-updates=ON
gtid-mode=ON
enforce-gtid-consistency=ON
```

### 3. Restart MySQL Server

**Linux (systemd):**
```bash
sudo systemctl restart mysql
# or
sudo systemctl restart mysqld
```

**macOS (Homebrew):**
```bash
brew services restart mysql@5.6
```

**Windows:**
- Use Services Manager to restart MySQL service

### 4. Verify Configuration

Connect to MySQL and verify settings:

```sql
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'log_slave_updates';
SHOW VARIABLES LIKE 'gtid_mode';
SHOW VARIABLES LIKE 'enforce_gtid_consistency';
```

Expected output:
- `log_bin` should be `ON`
- `log_slave_updates` should be `ON`
- `gtid_mode` should be `ON`
- `enforce_gtid_consistency` should be `ON`

### 5. Check GTID Status

```sql
SHOW MASTER STATUS;
```

This should show GTID information if GTID is properly enabled.

## Troubleshooting

### If log-bin is not enabled:
```sql
-- Check current value
SHOW VARIABLES LIKE 'log_bin';

-- Note: log-bin can only be set in config file, not dynamically
-- Add log-bin=mysql-bin to my.cnf and restart MySQL
```

### If log-slave-updates is not enabled:
```sql
-- Check current value
SHOW VARIABLES LIKE 'log_slave_updates';

-- Can be set dynamically (but should be in config for persistence)
SET GLOBAL log_slave_updates = ON;

-- Or add to config file:
-- log-slave-updates=ON
```

### If GTID mode cannot be enabled:
1. Ensure both `log-bin` and `log-slave-updates` are `ON`
2. Restart MySQL server
3. Try enabling GTID again

## Alternative: Use File:Position Format

If you don't need GTID, you can use the traditional file:position format:

```ini
[mysqld]
log-bin=mysql-bin
binlog-format=ROW
server-id=1
# GTID not required
```

Then set `use_gtid: false` in your `config.yaml`.

