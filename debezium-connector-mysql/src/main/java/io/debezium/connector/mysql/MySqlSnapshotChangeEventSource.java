/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.io.UnsupportedEncodingException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotLockingMode;
import io.debezium.connector.mysql.legacy.MySqlJdbcContext.DatabaseLocales;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

public class MySqlSnapshotChangeEventSource extends RelationalSnapshotChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlSnapshotChangeEventSource.class);

    private final MySqlConnectorConfig connectorConfig;
    private final MySqlConnection connection;
    private long globalLockAcquiredAt = -1;
    private final RelationalTableFilters filters;
    private final MySqlSnapshotChangeEventSourceMetrics metrics;
    private final MySqlOffsetContext previousOffset;
    private final MySqlDatabaseSchema databaseSchema;
    private final List<SchemaChangeEvent> schemaEvents = new ArrayList<>();

    public MySqlSnapshotChangeEventSource(MySqlConnectorConfig connectorConfig, MySqlOffsetContext previousOffset, MySqlConnection connection,
                                          MySqlDatabaseSchema schema, EventDispatcher<TableId> dispatcher, Clock clock,
                                          MySqlSnapshotChangeEventSourceMetrics metrics) {
        super(connectorConfig, previousOffset, connection, schema, dispatcher, clock, metrics);
        this.connectorConfig = connectorConfig;
        this.connection = connection;
        this.filters = connectorConfig.getTableFilters();
        this.metrics = metrics;
        this.previousOffset = previousOffset;
        this.databaseSchema = schema;
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(OffsetContext previousOffset) {
        boolean snapshotSchema = true;
        boolean snapshotData = true;

        // found a previous offset and the earlier snapshot has completed
        if (previousOffset != null && !previousOffset.isSnapshotRunning()) {
            LOGGER.info("A previous offset indicating a completed snapshot has been found. Neither schema nor data will be snapshotted.");
            snapshotSchema = false;
            snapshotData = false;
        }
        else {
            LOGGER.info("No previous offset has been found");
            if (connectorConfig.getSnapshotMode().includeData()) {
                LOGGER.info("According to the connector configuration both schema and data will be snapshotted");
            }
            else {
                LOGGER.info("According to the connector configuration only schema will be snapshotted");
            }
            snapshotData = connectorConfig.getSnapshotMode().includeData();
            snapshotSchema = connectorConfig.getSnapshotMode().includeSchema();
        }

        return new SnapshottingTask(snapshotSchema, snapshotData);
    }

    @Override
    protected SnapshotContext prepare(ChangeEventSourceContext context) throws Exception {
        return new MySqlSnapshotContext();
    }

    @Override
    protected void connectionCreated(RelationalSnapshotContext snapshotContext) throws Exception {
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext ctx) throws Exception {
        // -------------------
        // READ DATABASE NAMES
        // -------------------
        // Get the list of databases ...
        LOGGER.info("Read list of available databases");
        final List<String> databaseNames = new ArrayList<>();
        connection.query("SHOW DATABASES", rs -> {
            while (rs.next()) {
                databaseNames.add(rs.getString(1));
            }
        });
        LOGGER.info("\t list of available databases is: {}", databaseNames);

        // ----------------
        // READ TABLE NAMES
        // ----------------
        // Get the list of table IDs for each database. We can't use a prepared statement with MySQL, so we have to
        // build the SQL statement each time. Although in other cases this might lead to SQL injection, in our case
        // we are reading the database names from the database and not taking them from the user ...
        LOGGER.info("Read list of available tables in each database");
        final Set<TableId> tableIds = new HashSet<>();
        final Set<String> readableDatabaseNames = new HashSet<>();
        for (String dbName : databaseNames) {
            try {
                // MySQL sometimes considers some local files as databases (see DBZ-164),
                // so we will simply try each one and ignore the problematic ones ...
                connection.query("SHOW FULL TABLES IN " + quote(dbName) + " where Table_Type = 'BASE TABLE'", rs -> {
                    while (rs.next()) {
                        TableId id = new TableId(dbName, null, rs.getString(1));
                        tableIds.add(id);
                    }
                });
                readableDatabaseNames.add(dbName);
            }
            catch (SQLException e) {
                // We were unable to execute the query or process the results, so skip this ...
                LOGGER.warn("\t skipping database '{}' due to error reading tables: {}", dbName, e.getMessage());
            }
        }
        final Set<String> includedDatabaseNames = readableDatabaseNames.stream().filter(filters.databaseFilter()).collect(Collectors.toSet());
        LOGGER.info("\tsnapshot continuing with database(s): {}", includedDatabaseNames);
        return tableIds;
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext, RelationalSnapshotContext snapshotContext)
            throws SQLException, InterruptedException {
        // Set the transaction isolation level to REPEATABLE READ. This is the default, but the default can be changed
        // which is why we explicitly set it here.
        //
        // With REPEATABLE READ, all SELECT queries within the scope of a transaction (which we don't yet have) will read
        // from the same MVCC snapshot. Thus each plain (non-locking) SELECT statements within the same transaction are
        // consistent also with respect to each other.
        //
        // See: https://dev.mysql.com/doc/refman/5.7/en/set-transaction.html
        // See: https://dev.mysql.com/doc/refman/5.7/en/innodb-transaction-isolation-levels.html
        // See: https://dev.mysql.com/doc/refman/5.7/en/innodb-consistent-read.html
        connection.connection().setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        connection.executeWithoutCommitting("SET SESSION lock_wait_timeout=" + connectorConfig.snapshotLockTimeout().getSeconds());
        try {
            connection.executeWithoutCommitting("SET SESSION innodb_lock_wait_timeout=" + connectorConfig.snapshotLockTimeout().getSeconds());
        }
        catch (SQLException e) {
            LOGGER.warn("Unable to set innodb_lock_wait_timeout", e);
        }

        // ------------------------------------
        // LOCK TABLES
        // ------------------------------------
        // Obtain read lock on all tables. This statement closes all open tables and locks all tables
        // for all databases with a global read lock, and it prevents ALL updates while we have this lock.
        // It also ensures that everything we do while we have this lock will be consistent.
        if (connectorConfig.getSnapshotLockingMode() != MySqlConnectorConfig.SnapshotLockingMode.NONE) {
            try {
                globalLock();
                metrics.globalLockAcquired();
            }
            catch (SQLException e) {
                LOGGER.info("Unable to flush and acquire global read lock, will use table read locks after reading table names");
                // Continue anyway, since RDS (among others) don't allow setting a global lock
                assert !isGloballyLocked();
            }
            // FLUSH TABLES resets TX and isolation level
            connection.executeWithoutCommitting("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
        }
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext snapshotContext) throws SQLException {
        if (connectorConfig.getSnapshotLockingMode() == SnapshotLockingMode.MINIMAL) {
            if (isGloballyLocked()) {
                globalUnlock();
            }
        }
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext ctx) throws Exception {
        if (previousOffset != null) {
            ctx.offset = previousOffset;
            tryStartingSnapshot(ctx);
            return;
        }
        final MySqlOffsetContext offsetContext = MySqlOffsetContext.initial(connectorConfig);
        ctx.offset = offsetContext;
        LOGGER.info("Read binlog position of MySQL primary server");
        final String showMasterStmt = "SHOW MASTER STATUS";
        connection.query(showMasterStmt, rs -> {
            if (rs.next()) {
                final String binlogFilename = rs.getString(1);
                final long binlogPosition = rs.getLong(2);
                offsetContext.setBinlogStartPoint(binlogFilename, binlogPosition);
                if (rs.getMetaData().getColumnCount() > 4) {
                    // This column exists only in MySQL 5.6.5 or later ...
                    final String gtidSet = rs.getString(5); // GTID set, may be null, blank, or contain a GTID set
                    offsetContext.setCompletedGtidSet(gtidSet);
                    LOGGER.info("\t using binlog '{}' at position '{}' and gtid '{}'", binlogFilename, binlogPosition,
                            gtidSet);
                }
                else {
                    LOGGER.info("\t using binlog '{}' at position '{}'", binlogFilename, binlogPosition);
                }
            }
            else {
                throw new DebeziumException("Cannot read the binlog filename and position via '" + showMasterStmt
                        + "'. Make sure your server is correctly configured");
            }
        });
        tryStartingSnapshot(ctx);
    }

    private void addSchemaEvent(RelationalSnapshotContext snapshotContext, String database, String ddl) {
        schemaEvents.addAll(databaseSchema.parseSnapshotDdl(ddl, database, (MySqlOffsetContext) snapshotContext.offset,
                clock.currentTimeAsInstant()));
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext, RelationalSnapshotContext snapshotContext) throws SQLException, InterruptedException {
        Set<TableId> capturedSchemaTables;
        if (databaseSchema.storeOnlyMonitoredTables()) {
            capturedSchemaTables = snapshotContext.capturedTables;
            LOGGER.info("Only monitored tables schema should be captured, capturing: {}", capturedSchemaTables);
        }
        else {
            capturedSchemaTables = snapshotContext.capturedSchemaTables;
            LOGGER.info("All eligible tables schema should be captured, capturing: {}", capturedSchemaTables);
        }
        final Map<String, List<TableId>> tablesToRead = capturedSchemaTables.stream()
                .collect(Collectors.groupingBy(TableId::catalog, LinkedHashMap::new, Collectors.toList()));
        final Set<String> databases = tablesToRead.keySet();

        // Record default charset
        addSchemaEvent(snapshotContext, "", connection.setStatementFor(connection.readMySqlCharsetSystemVariables()));

        for (TableId tableId : capturedSchemaTables) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while emitting initial DROP TABLE events");
            }
            addSchemaEvent(snapshotContext, tableId.catalog(), "DROP TABLE IF EXISTS " + quote(tableId));
        }

        final Map<String, DatabaseLocales> databaseCharsets = connection.readDatabaseCollations();
        for (String database : databases) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while reading structure of schema " + databases);
            }

            LOGGER.info("Reading structure of database '{}'", database);
            addSchemaEvent(snapshotContext, database, "DROP DATABASE IF EXISTS " + quote(database));
            final StringBuilder createDatabaseDddl = new StringBuilder("CREATE DATABASE " + quote(database));
            final DatabaseLocales defaultDatabaseLocales = databaseCharsets.get(database);
            if (defaultDatabaseLocales != null) {
                defaultDatabaseLocales.appendToDdlStatement(database, createDatabaseDddl);
            }
            addSchemaEvent(snapshotContext, database, createDatabaseDddl.toString());
            addSchemaEvent(snapshotContext, database, "USE " + quote(database));

            for (TableId tableId : tablesToRead.get(database)) {
                connection.query("SHOW CREATE TABLE " + quote(tableId), rs -> {
                    if (rs.next()) {
                        addSchemaEvent(snapshotContext, database, rs.getString(2));
                    }
                });
            }
        }
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext snapshotContext, Table table) throws SQLException {
        return new SchemaChangeEvent(
                snapshotContext.offset.getPartition(),
                snapshotContext.offset.getOffset(),
                snapshotContext.offset.getSourceInfo(),
                snapshotContext.catalogName,
                table.id().schema(),
                null,
                table,
                SchemaChangeEventType.CREATE,
                true);
    }

    @Override
    protected void complete(SnapshotContext snapshotContext) {
    }

    /**
     * Generate a valid sqlserver query string for the specified table
     *
     * @param tableId the table to generate a query for
     * @return a valid query string
     */
    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext snapshotContext, TableId tableId) {
        return Optional.of(String.format("SELECT * FROM `%s`.`%s`", tableId.catalog(), tableId.table()));
    }

    @Override
    protected Object getColumnValue(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException {
        if (column.jdbcType() == Types.TIME) {
            return readTimeField(rs, columnIndex);
        }
        else if (column.jdbcType() == Types.DATE) {
            return readDateField(rs, columnIndex, column, table);
        }
        // This is for DATETIME columns (a logical date + time without time zone)
        // by reading them with a calendar based on the default time zone, we make sure that the value
        // is constructed correctly using the database's (or connection's) time zone
        else if (column.jdbcType() == Types.TIMESTAMP) {
            return readTimestampField(rs, columnIndex, column, table);
        }
        // JDBC's rs.GetObject() will return a Boolean for all TINYINT(1) columns.
        // TINYINT columns are reported as SMALLINT by JDBC driver
        else if (column.jdbcType() == Types.TINYINT || column.jdbcType() == Types.SMALLINT) {
            // It seems that rs.wasNull() returns false when default value is set and NULL is inserted
            // We thus need to use getObject() to identify if the value was provided and if yes then
            // read it again to get correct scale
            return rs.getObject(columnIndex) == null ? null : rs.getInt(columnIndex);
        }
        // DBZ-2673
        // It is necessary to check the type names as types like ENUM and SET are
        // also reported as JDBC type char
        else if ("CHAR".equals(column.typeName()) ||
                "VARCHAR".equals(column.typeName()) ||
                "TEXT".equals(column.typeName())) {
            return rs.getBytes(columnIndex);
        }
        else {
            return rs.getObject(columnIndex);
        }
    }

    /**
     * As MySQL connector/J implementation is broken for MySQL type "TIME" we have to use a binary-ish workaround
     *
     * @see https://issues.jboss.org/browse/DBZ-342
     */
    private Object readTimeField(ResultSet rs, int fieldNo) throws SQLException {
        Blob b = rs.getBlob(fieldNo);
        if (b == null) {
            return null; // Don't continue parsing time field if it is null
        }

        try {
            return MySqlValueConverters.stringToDuration(new String(b.getBytes(1, (int) (b.length())), "UTF-8"));
        }
        catch (UnsupportedEncodingException e) {
            LOGGER.error("Could not read MySQL TIME value as UTF-8");
            throw new RuntimeException(e);
        }
    }

    /**
     * In non-string mode the date field can contain zero in any of the date part which we need to handle as all-zero
     *
     */
    private Object readDateField(ResultSet rs, int fieldNo, Column column, Table table) throws SQLException {
        Blob b = rs.getBlob(fieldNo);
        if (b == null) {
            return null; // Don't continue parsing date field if it is null
        }

        try {
            return MySqlValueConverters.stringToLocalDate(new String(b.getBytes(1, (int) (b.length())), "UTF-8"), column, table);
        }
        catch (UnsupportedEncodingException e) {
            LOGGER.error("Could not read MySQL TIME value as UTF-8");
            throw new RuntimeException(e);
        }
    }

    /**
     * In non-string mode the time field can contain zero in any of the date part which we need to handle as all-zero
     *
     */
    private Object readTimestampField(ResultSet rs, int fieldNo, Column column, Table table) throws SQLException {
        Blob b = rs.getBlob(fieldNo);
        if (b == null) {
            return null; // Don't continue parsing timestamp field if it is null
        }

        try {
            return MySqlValueConverters.containsZeroValuesInDatePart((new String(b.getBytes(1, (int) (b.length())), "UTF-8")), column, table) ? null
                    : rs.getTimestamp(fieldNo, Calendar.getInstance());
        }
        catch (UnsupportedEncodingException e) {
            LOGGER.error("Could not read MySQL TIME value as UTF-8");
            throw new RuntimeException(e);
        }
    }

    private boolean isGloballyLocked() {
        return globalLockAcquiredAt != -1;
    }

    private void globalUnlock() throws SQLException {
        LOGGER.info("Releasing global read lock to enable MySQL writes");
        connection.executeWithoutCommitting("UNLOCK TABLES");
        long lockReleased = clock.currentTimeInMillis();
        metrics.globalLockReleased();
        LOGGER.info("Writes to MySQL tables prevented for a total of {}", Strings.duration(lockReleased - globalLockAcquiredAt));
        globalLockAcquiredAt = -1;
    }

    private void globalLock() throws SQLException {
        LOGGER.info("Flush and obtain global read lock to prevent writes to database");
        connection.executeWithoutCommitting("FLUSH TABLES WITH READ LOCK");
        globalLockAcquiredAt = clock.currentTimeInMillis();
    }

    private String quote(String dbOrTableName) {
        return "`" + dbOrTableName + "`";
    }

    private String quote(TableId id) {
        return quote(id.catalog()) + "." + quote(id.table());
    }

    @Override
    protected OptionalLong rowCountForTable(TableId tableId) {
        return connection.getEstimatedTableSize(tableId);
    }

    @Override
    protected Statement readTableStatement(OptionalLong rowCount) throws SQLException {
        final long largeTableRowCount = connectorConfig.rowCountForLargeTable();
        if (!rowCount.isPresent() || largeTableRowCount == 0 || rowCount.getAsLong() <= largeTableRowCount) {
            return super.readTableStatement(rowCount);
        }
        return createStatementWithLargeResultSet();
    }

    /**
     * Create a JDBC statement that can be used for large result sets.
     * <p>
     * By default, the MySQL Connector/J driver retrieves all rows for ResultSets and stores them in memory. In most cases this
     * is the most efficient way to operate and, due to the design of the MySQL network protocol, is easier to implement.
     * However, when ResultSets that have a large number of rows or large values, the driver may not be able to allocate
     * heap space in the JVM and may result in an {@link OutOfMemoryError}. See
     * <a href="https://issues.jboss.org/browse/DBZ-94">DBZ-94</a> for details.
     * <p>
     * This method handles such cases using the
     * <a href="https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-implementation-notes.html">recommended
     * technique</a> for MySQL by creating the JDBC {@link Statement} with {@link ResultSet#TYPE_FORWARD_ONLY forward-only} cursor
     * and {@link ResultSet#CONCUR_READ_ONLY read-only concurrency} flags, and with a {@link Integer#MIN_VALUE minimum value}
     * {@link Statement#setFetchSize(int) fetch size hint}.
     *
     * @return the statement; never null
     * @throws SQLException if there is a problem creating the statement
     */
    private Statement createStatementWithLargeResultSet() throws SQLException {
        int fetchSize = connectorConfig.getSnapshotFetchSize();
        Statement stmt = connection.connection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(fetchSize);
        return stmt;
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private static class MySqlSnapshotContext extends RelationalSnapshotContext {

        public MySqlSnapshotContext() throws SQLException {
            super("");
        }
    }

    @Override
    protected void createSchemaChangeEventsForTables(ChangeEventSourceContext sourceContext,
                                                     RelationalSnapshotContext snapshotContext, SnapshottingTask snapshottingTask)
            throws Exception {
        tryStartingSnapshot(snapshotContext);

        for (Iterator<SchemaChangeEvent> i = schemaEvents.iterator(); i.hasNext();) {
            final SchemaChangeEvent event = i.next();
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while processing raw event " + event);
            }

            if (databaseSchema.storeOnlyMonitoredTables() && event.getDatabase() != null && event.getDatabase().length() != 0
                    && !connectorConfig.getTableFilters().databaseFilter().test(event.getDatabase())) {
                LOGGER.debug("Skipping schema event as it belongs to a non-captured database: '{}'", event);
                continue;
            }

            LOGGER.debug("Processing schema event {}", event);

            final TableId tableId = event.getTables().isEmpty() ? null : event.getTables().iterator().next().id();
            snapshotContext.offset.event(tableId, getClock().currentTime());

            // If data are not snapshotted then the last schema change must set last snapshot flag
            if (!snapshottingTask.snapshotData() && !i.hasNext()) {
                snapshotContext.offset.markLastSnapshotRecord();
            }
            try {
                dispatcher.dispatchSchemaChangeEvent(tableId, (receiver) -> {
                    try {
                        receiver.schemaChangeEvent(event);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // Make schema available for snapshot source
        databaseSchema.tableIds().forEach(x -> snapshotContext.tables.overwriteTable(databaseSchema.tableFor(x)));
    }
}
