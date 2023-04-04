/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.mysql.MySqlConnection.DatabaseLocales;
import io.debezium.connector.mysql.MySqlOffsetContext.Loader;
import io.debezium.data.Envelope;
import io.debezium.function.BlockingConsumer;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.util.Clock;
import io.debezium.util.Collect;
import io.debezium.util.Strings;

public class MySqlSnapshotChangeEventSource extends RelationalSnapshotChangeEventSource<MySqlPartition, MySqlOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlSnapshotChangeEventSource.class);

    private final MySqlConnectorConfig connectorConfig;
    private final MySqlConnection connection;
    private long globalLockAcquiredAt = -1;
    private long tableLockAcquiredAt = -1;
    private final RelationalTableFilters filters;
    private final MySqlSnapshotChangeEventSourceMetrics metrics;
    private final MySqlDatabaseSchema databaseSchema;
    private final List<SchemaChangeEvent> schemaEvents = new ArrayList<>();
    private Set<TableId> delayedSchemaSnapshotTables = Collections.emptySet();
    private final BlockingConsumer<Function<SourceRecord, SourceRecord>> lastEventProcessor;

    public MySqlSnapshotChangeEventSource(MySqlConnectorConfig connectorConfig, MainConnectionProvidingConnectionFactory<MySqlConnection> connectionFactory,
                                          MySqlDatabaseSchema schema, EventDispatcher<MySqlPartition, TableId> dispatcher, Clock clock,
                                          MySqlSnapshotChangeEventSourceMetrics metrics,
                                          BlockingConsumer<Function<SourceRecord, SourceRecord>> lastEventProcessor) {
        super(connectorConfig, connectionFactory, schema, dispatcher, clock, metrics);
        this.connectorConfig = connectorConfig;
        this.connection = connectionFactory.mainConnection();
        this.filters = connectorConfig.getTableFilters();
        this.metrics = metrics;
        this.databaseSchema = schema;
        this.lastEventProcessor = lastEventProcessor;
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(MySqlPartition partition, MySqlOffsetContext previousOffset) {
        boolean snapshotSchema = true;
        boolean snapshotData = true;

        // found a previous offset and the earlier snapshot has completed
        if (previousOffset != null && !previousOffset.isSnapshotRunning()) {
            LOGGER.info("A previous offset indicating a completed snapshot has been found. Neither schema nor data will be snapshotted.");
            snapshotSchema = databaseSchema.isStorageInitializationExecuted();
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
    protected SnapshotContext<MySqlPartition, MySqlOffsetContext> prepare(MySqlPartition partition) throws Exception {
        return new MySqlSnapshotContext(partition);
    }

    @Override
    protected void connectionCreated(RelationalSnapshotContext<MySqlPartition, MySqlOffsetContext> snapshotContext)
            throws Exception {
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<MySqlPartition, MySqlOffsetContext> ctx)
            throws Exception {
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
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<MySqlPartition, MySqlOffsetContext> snapshotContext)
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
        if (connectorConfig.getSnapshotLockingMode().usesLocking() && connectorConfig.useGlobalLock()) {
            try {
                globalLock();
                metrics.globalLockAcquired();
            }
            catch (SQLException e) {
                LOGGER.info("Unable to flush and acquire global read lock, will use table read locks after reading table names");
                // Continue anyway, since RDS (among others) don't allow setting a global lock
                assert !isGloballyLocked();
            }
            if (connectorConfig.getSnapshotLockingMode().flushResetsIsolationLevel()) {
                // FLUSH TABLES resets TX and isolation level
                connection.executeWithoutCommitting("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            }
        }
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<MySqlPartition, MySqlOffsetContext> snapshotContext)
            throws SQLException {
        if (connectorConfig.getSnapshotLockingMode().usesMinimalLocking()) {
            if (isGloballyLocked()) {
                globalUnlock();
            }
            if (isTablesLocked()) {
                // We could not acquire a global read lock and instead had to obtain individual table-level read locks
                // using 'FLUSH TABLE <tableName> WITH READ LOCK'. However, if we were to do this, the 'UNLOCK TABLES'
                // would implicitly commit our active transaction, and this would break our consistent snapshot logic.
                // Therefore, we cannot unlock the tables here!
                // https://dev.mysql.com/doc/refman/5.7/en/flush.html
                LOGGER.warn("Tables were locked explicitly, but to get a consistent snapshot we cannot release the locks until we've read all tables.");
            }
        }
    }

    @Override
    protected void releaseDataSnapshotLocks(RelationalSnapshotContext<MySqlPartition, MySqlOffsetContext> snapshotContext)
            throws Exception {
        if (isGloballyLocked()) {
            globalUnlock();
        }
        if (isTablesLocked()) {
            tableUnlock();
            if (!delayedSchemaSnapshotTables.isEmpty()) {
                schemaEvents.clear();
                createSchemaEventsForTables(snapshotContext, delayedSchemaSnapshotTables, false);

                for (Iterator<SchemaChangeEvent> i = schemaEvents.iterator(); i.hasNext();) {
                    final SchemaChangeEvent event = i.next();

                    if (databaseSchema.storeOnlyCapturedTables() && event.getDatabase() != null && event.getDatabase().length() != 0
                            && !connectorConfig.getTableFilters().databaseFilter().test(event.getDatabase())) {
                        LOGGER.debug("Skipping schema event as it belongs to a non-captured database: '{}'", event);
                        continue;
                    }

                    LOGGER.debug("Processing schema event {}", event);

                    final TableId tableId = event.getTables().isEmpty() ? null : event.getTables().iterator().next().id();
                    snapshotContext.offset.event(tableId, getClock().currentTime());
                    dispatcher.dispatchSchemaChangeEvent(snapshotContext.partition, tableId, (receiver) -> receiver.schemaChangeEvent(event));
                }

                // Make schema available for snapshot source
                databaseSchema.tableIds().forEach(x -> snapshotContext.tables.overwriteTable(databaseSchema.tableFor(x)));
            }
        }
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<MySqlPartition, MySqlOffsetContext> ctx,
                                           MySqlOffsetContext previousOffset)
            throws Exception {
        if (!isGloballyLocked() && !isTablesLocked() && connectorConfig.getSnapshotLockingMode().usesLocking()) {
            return;
        }
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
            else if (!connectorConfig.getSnapshotMode().shouldStream()) {
                LOGGER.warn("Failed retrieving binlog position, continuing as streaming CDC wasn't requested");
            }
            else {
                throw new DebeziumException("Cannot read the binlog filename and position via '" + showMasterStmt
                        + "'. Make sure your server is correctly configured");
            }
        });
        tryStartingSnapshot(ctx);
    }

    private void addSchemaEvent(RelationalSnapshotContext<MySqlPartition, MySqlOffsetContext> snapshotContext,
                                String database, String ddl) {
        schemaEvents.addAll(databaseSchema.parseSnapshotDdl(snapshotContext.partition, ddl, database,
                snapshotContext.offset, clock.currentTimeAsInstant()));
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext,
                                      RelationalSnapshotContext<MySqlPartition, MySqlOffsetContext> snapshotContext,
                                      MySqlOffsetContext offsetContext)
            throws Exception {
        Set<TableId> capturedSchemaTables;
        if (twoPhaseSchemaSnapshot()) {
            // Capture schema of captured tables after they are locked
            tableLock(snapshotContext);
            determineSnapshotOffset(snapshotContext, offsetContext);
            capturedSchemaTables = snapshotContext.capturedTables;
            LOGGER.info("Table level locking is in place, the schema will be capture in two phases, now capturing: {}", capturedSchemaTables);
            delayedSchemaSnapshotTables = Collect.minus(snapshotContext.capturedSchemaTables, snapshotContext.capturedTables);
            LOGGER.info("Tables for delayed schema capture: {}", delayedSchemaSnapshotTables);
        }
        if (databaseSchema.storeOnlyCapturedTables()) {
            capturedSchemaTables = snapshotContext.capturedTables;
            LOGGER.info("Only captured tables schema should be captured, capturing: {}", capturedSchemaTables);
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

            createSchemaEventsForTables(snapshotContext, tablesToRead.get(database), true);
        }
    }

    void createSchemaEventsForTables(RelationalSnapshotContext<MySqlPartition, MySqlOffsetContext> snapshotContext,
                                     final Collection<TableId> tablesToRead, final boolean firstPhase)
            throws SQLException {
        for (TableId tableId : tablesToRead) {
            if (firstPhase && delayedSchemaSnapshotTables.contains(tableId)) {
                continue;
            }
            connection.query("SHOW CREATE TABLE " + quote(tableId), rs -> {
                if (rs.next()) {
                    addSchemaEvent(snapshotContext, tableId.catalog(), rs.getString(2));
                }
            });
        }
    }

    private boolean twoPhaseSchemaSnapshot() {
        return connectorConfig.getSnapshotLockingMode().usesLocking() && !isGloballyLocked();
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<MySqlPartition, MySqlOffsetContext> snapshotContext,
                                                    Table table)
            throws SQLException {
        return SchemaChangeEvent.ofSnapshotCreate(snapshotContext.partition, snapshotContext.offset, snapshotContext.catalogName, table);
    }

    /**
     * Generate a valid MySQL query string for the specified table and columns
     *
     * @param tableId the table to generate a query for
     * @return a valid query string
     */
    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<MySqlPartition, MySqlOffsetContext> snapshotContext,
                                                 TableId tableId, List<String> columns) {
        return Optional.of(getSnapshotSelect(tableId, columns));
    }

    private String getSnapshotSelect(TableId tableId, List<String> columns) {
        String snapshotSelectColumns = columns.stream().collect(Collectors.joining(", "));
        return String.format("SELECT %s FROM `%s`.`%s`", snapshotSelectColumns, tableId.catalog(), tableId.table());
    }

    @Override
    protected Optional<String> getSnapshotConnectionFirstSelect(RelationalSnapshotContext<MySqlPartition, MySqlOffsetContext> snapshotContext, TableId tableId) {
        return Optional.of(getSnapshotSelect(tableId, Arrays.asList("*")) + " LIMIT 1");
    }

    private boolean isGloballyLocked() {
        return globalLockAcquiredAt != -1;
    }

    private boolean isTablesLocked() {
        return tableLockAcquiredAt != -1;
    }

    private void globalLock() throws SQLException {
        LOGGER.info("Flush and obtain global read lock to prevent writes to database");
        connection.executeWithoutCommitting(connectorConfig.getSnapshotLockingMode().getLockStatement());
        globalLockAcquiredAt = clock.currentTimeInMillis();
    }

    private void globalUnlock() throws SQLException {
        LOGGER.info("Releasing global read lock to enable MySQL writes");
        connection.executeWithoutCommitting("UNLOCK TABLES");
        long lockReleased = clock.currentTimeInMillis();
        metrics.globalLockReleased();
        LOGGER.info("Writes to MySQL tables prevented for a total of {}", Strings.duration(lockReleased - globalLockAcquiredAt));
        globalLockAcquiredAt = -1;
    }

    private void tableLock(RelationalSnapshotContext<MySqlPartition, MySqlOffsetContext> snapshotContext)
            throws SQLException {
        // ------------------------------------
        // LOCK TABLES and READ BINLOG POSITION
        // ------------------------------------
        // We were not able to acquire the global read lock, so instead we have to obtain a read lock on each table.
        // This requires different privileges than normal, and also means we can't unlock the tables without
        // implicitly committing our transaction ...
        if (!connection.userHasPrivileges("LOCK TABLES")) {
            // We don't have the right privileges
            throw new DebeziumException("User does not have the 'LOCK TABLES' privilege required to obtain a "
                    + "consistent snapshot by preventing concurrent writes to tables.");
        }
        // We have the required privileges, so try to lock all of the tables we're interested in ...
        LOGGER.info("Flush and obtain read lock for {} tables (preventing writes)", snapshotContext.capturedTables);
        if (!snapshotContext.capturedTables.isEmpty()) {
            final String tableList = snapshotContext.capturedTables.stream()
                    .map(tid -> quote(tid))
                    .collect(Collectors.joining(","));
            connection.executeWithoutCommitting("FLUSH TABLES " + tableList + " WITH READ LOCK");
        }
        tableLockAcquiredAt = clock.currentTimeInMillis();
        metrics.globalLockAcquired();
    }

    private void tableUnlock() throws SQLException {
        LOGGER.info("Releasing table read lock to enable MySQL writes");
        connection.executeWithoutCommitting("UNLOCK TABLES");
        long lockReleased = clock.currentTimeInMillis();
        metrics.globalLockReleased();
        LOGGER.info("Writes to MySQL tables prevented for a total of {}", Strings.duration(lockReleased - tableLockAcquiredAt));
        tableLockAcquiredAt = -1;
    }

    private String quote(String dbOrTableName) {
        return "`" + dbOrTableName + "`";
    }

    private String quote(TableId id) {
        return quote(id.catalog()) + "." + quote(id.table());
    }

    @Override
    protected OptionalLong rowCountForTable(TableId tableId) {
        if (getSnapshotSelectOverridesByTable(tableId) != null) {
            return super.rowCountForTable(tableId);
        }
        OptionalLong rowCount = connection.getEstimatedTableSize(tableId);
        LOGGER.info("Estimated row count for table {} is {}", tableId, rowCount);
        return rowCount;
    }

    @Override
    protected Statement readTableStatement(JdbcConnection jdbcConnection, OptionalLong rowCount) throws SQLException {
        MySqlConnection connection = (MySqlConnection) jdbcConnection;
        final long largeTableRowCount = connectorConfig.rowCountForLargeTable();
        if (!rowCount.isPresent() || largeTableRowCount == 0 || rowCount.getAsLong() <= largeTableRowCount) {
            return super.readTableStatement(connection, rowCount);
        }
        return createStatementWithLargeResultSet(connection);
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
    private Statement createStatementWithLargeResultSet(MySqlConnection connection) throws SQLException {
        int fetchSize = connectorConfig.getSnapshotFetchSize();
        Statement stmt = connection.connection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(fetchSize);
        return stmt;
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private static class MySqlSnapshotContext extends RelationalSnapshotContext<MySqlPartition, MySqlOffsetContext> {

        MySqlSnapshotContext(MySqlPartition partition) throws SQLException {
            super(partition, "");
        }
    }

    @Override
    protected void createSchemaChangeEventsForTables(ChangeEventSourceContext sourceContext,
                                                     RelationalSnapshotContext<MySqlPartition, MySqlOffsetContext> snapshotContext,
                                                     SnapshottingTask snapshottingTask)
            throws Exception {
        tryStartingSnapshot(snapshotContext);

        for (Iterator<SchemaChangeEvent> i = schemaEvents.iterator(); i.hasNext();) {
            final SchemaChangeEvent event = i.next();
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while processing event " + event);
            }

            if (databaseSchema.skipSchemaChangeEvent(event)) {
                continue;
            }

            LOGGER.debug("Processing schema event {}", event);

            final TableId tableId = event.getTables().isEmpty() ? null : event.getTables().iterator().next().id();
            snapshotContext.offset.event(tableId, getClock().currentTime());
            dispatcher.dispatchSchemaChangeEvent(snapshotContext.partition, tableId, (receiver) -> receiver.schemaChangeEvent(event));
        }

        // Make schema available for snapshot source
        databaseSchema.tableIds().forEach(x -> snapshotContext.tables.overwriteTable(databaseSchema.tableFor(x)));
    }

    @Override
    protected void postSnapshot() throws InterruptedException {
        // We cannot be sure that the last event as the last one
        // - last table could be empty
        // - data snapshot was not executed
        // - the last table schema snaphsotted is not monitored and storing of monitored is disabled
        lastEventProcessor.accept(record -> {
            record.sourceOffset().remove(SourceInfo.SNAPSHOT_KEY);
            ((Struct) record.value()).getStruct(Envelope.FieldName.SOURCE).put(SourceInfo.SNAPSHOT_KEY, SnapshotRecord.LAST.toString().toLowerCase());
            return record;
        });
        super.postSnapshot();
    }

    @Override
    protected MySqlOffsetContext copyOffset(RelationalSnapshotContext<MySqlPartition, MySqlOffsetContext> snapshotContext) {
        return new Loader(connectorConfig).load(snapshotContext.offset.getOffset());
    }
}
