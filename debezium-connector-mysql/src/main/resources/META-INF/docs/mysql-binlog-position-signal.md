# MySQL Set Binlog Position Signal

## Overview

The `set-binlog-position` signal allows you to dynamically change the binlog reading position of a running MySQL connector. This is useful for:

- **Disaster Recovery**: Skip to a known good position after system failures
- **Partial Replication**: Start CDC from recent data only, skipping years of history
- **Testing**: Test with specific datasets without full historical snapshots
- **Migration Scenarios**: Precise control over CDC starting positions

## How It Works

When the signal is received, the connector:
1. Updates its internal offset to the specified binlog position or GTID set
2. Commits the new offset to the offset storage
3. Restarts itself to begin streaming from the new position

## Signal Format

The signal supports two formats:

### Binlog File and Position

```json
{
  "type": "set-binlog-position",
  "data": {
    "binlog_filename": "mysql-bin.000003",
    "binlog_position": 1234
  }
}
```

### GTID Set

```json
{
  "type": "set-binlog-position",
  "data": {
    "gtid_set": "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100"
  }
}
```

## Sending the Signal

You can send the signal through any enabled signal channel:

### Source Signal Channel (Database Table)

```sql
INSERT INTO debezium_signal (id, type, data) VALUES (
  'position-change-001',
  'set-binlog-position',
  '{"binlog_filename": "mysql-bin.000003", "binlog_position": 1234}'
);
```

### Kafka Signal Channel

Send a message to the signal topic with the signal format.

### File Signal Channel

Write the signal to the configured signal file.

## Important Considerations

1. **Data Loss Risk**: Skipping binlog positions means you will miss any changes in the skipped range.

2. **Schema Consistency**: The connector uses the current schema, not the schema at the specified position. Ensure no schema changes occurred between the current position and target position.

3. **Restart Required**: The connector must restart to apply the new position, causing a brief interruption.

4. **One-Time Operation**: Unlike configuration properties, this signal performs a one-time position adjustment.

## Example Use Cases

### Disaster Recovery

After a failure, skip to the last known good position:

```sql
-- Find the last processed position from your monitoring
-- Then send signal to resume from that position
INSERT INTO debezium_signal (id, type, data) VALUES (
  'recovery-001',
  'set-binlog-position',
  '{"binlog_filename": "mysql-bin.000042", "binlog_position": 98765}'
);
```

### Skip Historical Data

Start capturing only recent changes:

```sql
-- Get current binlog position
SHOW MASTER STATUS;

-- Send signal to start from current position
INSERT INTO debezium_signal (id, type, data) VALUES (
  'skip-history-001',
  'set-binlog-position',
  '{"binlog_filename": "mysql-bin.000156", "binlog_position": 4321}'
);
```

### GTID-Based Positioning

For GTID-enabled MySQL servers:

```sql
-- Get current GTID set
SELECT @@GLOBAL.gtid_executed;

-- Send signal with GTID set
INSERT INTO debezium_signal (id, type, data) VALUES (
  'gtid-position-001',
  'set-binlog-position',
  '{"gtid_set": "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5000"}'
);
```

## Validation

The signal validates:
- Binlog filename must match MySQL naming pattern (e.g., `mysql-bin.000001`)
- Position must be non-negative
- Cannot specify both file/position and GTID set
- GTID set must be valid format

## Error Handling

If the signal is invalid or cannot be processed:
- An error is logged
- The connector continues with its current position
- No changes are made to the offset