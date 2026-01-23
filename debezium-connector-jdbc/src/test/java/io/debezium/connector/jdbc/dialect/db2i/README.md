# DB2i Dialect Testing

## Why No Integration/E2E Tests?

DB2i (IBM i / AS/400) cannot be tested with automated integration or e2e tests because:

- **No Docker/TestContainers Support**: IBM i operating system cannot run in containers
- **Proprietary Hardware**: Requires IBM Power Systems or cloud instances
- **Licensing**: Commercial licensing required; no free/community editions available
- **Infrastructure**: Unlike DB2 LUW, DB2i doesn't have readily available Docker images

## Testing Approach

### Automated Testing: Unit Tests

The `Db2iDatabaseDialectTest` class validates SQL generation logic without requiring a live database:

```bash
# Run unit tests
mvn test -Dtest=Db2iDatabaseDialectTest
```

**What's Tested:**
- ✅ MERGE statement structure (uses `SELECT FROM sysibm.sysdummy1`)
- ✅ CAST wrapping for parameter markers
- ✅ Table aliasing (TGT for target table)
- ✅ ON clause qualification
- ✅ SET clause structure
- ✅ Type handling (VARCHAR, INTEGER, DECIMAL, etc.)

### Manual Testing Required

Integration and e2e testing must be performed manually against a real IBM i system.

#### Prerequisites

1. **IBM i System Access** (version 7.3 or higher)
2. **JT400 JDBC Driver** (`jt400.jar`)
3. **Journaling Enabled** on target tables:
   ```
   STRJRNPF FILE(MYSCHEMA/MYTABLE) JRN(QGPL/QSQJRN)
   ```
4. **Connection Configuration**:
   ```properties
   connection.url=jdbc:as400://hostname/database;naming=system;transaction isolation=read committed
   hibernate.dialect=org.hibernate.dialect.Db2iDialect
   ```

#### Manual Test Checklist

- [ ] **Insert Mode**: New records inserted successfully
- [ ] **Upsert Mode**: MERGE statements execute without SQL errors
  - [ ] Inserts work when key doesn't exist
  - [ ] Updates work when key exists
  - [ ] No `SQL0584` (NULL/parameter in VALUES)
  - [ ] No `SQL0604` (invalid attributes/missing types)
  - [ ] No `SQL0203` (ambiguous column names)
  - [ ] No `SQL5001` (column qualifier undefined)
- [ ] **Delete Mode**: Records deleted correctly
- [ ] **Schema Evolution**: Column additions/modifications handled
- [ ] **Data Types**: All supported types work correctly
  - [ ] Numeric types (INTEGER, DECIMAL, etc.)
  - [ ] String types (VARCHAR, CHAR)
  - [ ] Date/Time types (DATE, TIME, TIMESTAMP)
  - [ ] Binary types (BLOB, CLOB)
- [ ] **Connection Stability**: Multiple operations without connection drops
- [ ] **Transaction Handling**: Commits and rollbacks work as expected
- [ ] **Error Handling**: SQL errors reported correctly

#### Sample Test Configuration

```json
{
  "name": "db2i-sink-connector",
  "config": {
    "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "connection.url": "jdbc:as400://your-ibm-i-host/YOURDB",
    "connection.username": "youruser",
    "connection.password": "yourpass",
    "insert.mode": "upsert",
    "delete.enabled": "true",
    "primary.key.mode": "record_key",
    "schema.evolution": "basic",
    "hibernate.dialect": "org.hibernate.dialect.Db2iDialect",
    "topics": "your.source.topic"
  }
}
```

## Key DB2i-Specific Implementation Details

The `Db2iDatabaseDialect` overrides several methods to comply with DB2i SQL requirements:

1. **MERGE Syntax** (`getUpsertStatement`):
   - Uses `SELECT ... FROM sysibm.sysdummy1` instead of `VALUES` clause
   - DB2i doesn't allow untyped parameter markers in VALUES
   
2. **Type Casting** (`columnQueryBindingFromField`):
   - All parameter markers wrapped with `CAST(? AS typename)`
   - Includes precision/scale for DECIMAL, VARCHAR lengths, etc.
   
3. **Table Aliasing** (`getUpsertStatement`):
   - Target table uses `TGT` alias
   - Source data uses `DAT` alias
   - Prevents ambiguous column name errors
   
4. **Clause Qualification** (`getOnClause` vs `getMergeDatClause`):
   - ON clause: Uses table qualification (`TGT.col = DAT.col`)
   - SET clause: Uses DAT qualification only (`col = DAT.col`)
   - DB2i differs from DB2 LUW in SET clause requirements

## Troubleshooting

### Common Errors

**SQL7008: Table not valid for operation**
- **Cause**: Journaling not enabled on table
- **Fix**: `STRJRNPF FILE(schema/table) JRN(QGPL/QSQJRN)`

**Connection drops after first write**
- **Cause**: Transaction isolation level incompatibility
- **Fix**: Set `transaction isolation=read committed` in connection URL

**SQL0584: NULL or parameter marker in VALUES**
- **Status**: Fixed in `Db2iDatabaseDialect` by using SELECT syntax

**SQL0604: Attributes not valid**
- **Status**: Fixed by adding explicit CAST with type sizes

## Contributing

If you have access to IBM i and can perform manual testing:

1. Run the manual test checklist above
2. Document any issues or edge cases discovered
3. Report findings in the Debezium JIRA/GitHub
4. Consider contributing additional unit tests for edge cases

## References

- [IBM i SQL Reference](https://www.ibm.com/docs/en/i/7.5?topic=overview-sql-reference)
- [Hibernate Db2iDialect Documentation](https://docs.jboss.org/hibernate/orm/current/javadocs/org/hibernate/dialect/Db2iDialect.html)
- [JT400 JDBC Driver](https://github.com/IBM/JTOpen)
