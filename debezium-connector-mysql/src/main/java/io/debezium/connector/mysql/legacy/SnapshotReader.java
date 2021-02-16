/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.legacy;

import java.io.UnsupportedEncodingException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.config.Configuration;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.connector.mysql.legacy.MySqlJdbcContext.DatabaseLocales;
import io.debezium.connector.mysql.legacy.RecordMakers.RecordsForTable;
import io.debezium.data.Envelope;
import io.debezium.function.BufferedBlockingConsumer;
import io.debezium.function.Predicates;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcConnection.StatementFactory;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import io.debezium.util.Threads;

/**
 * A component that performs a snapshot of a MySQL server, and records the schema changes in {@link MySqlSchema}.
 *
 * @author Randall Hauch
 */
public class SnapshotReader extends AbstractReader {

    private final boolean includeData;
    private RecordRecorder recorder;
    private final SnapshotReaderMetrics metrics;
    private ExecutorService executorService;
    private final boolean useGlobalLock;

    private final MySqlConnectorConfig.SnapshotLockingMode snapshotLockingMode;

    /**
     * Create a snapshot reader.
     *
     * @param name the name of this reader; may not be null
     * @param context the task context in which this reader is running; may not be null
     */
    public SnapshotReader(String name, MySqlTaskContext context) {
        this(name, context, true);
    }

    /**
     * Create a snapshot reader that can use global locking only optionally.
     * Used mostly for testing.
     *
     * @param name the name of this reader; may not be null
     * @param context the task context in which this reader is running; may not be null
     * @param useGlobalLock {@code false} to simulate cloud (Amazon RDS) restrictions
     */
    SnapshotReader(String name, MySqlTaskContext context, boolean useGlobalLock) {
        super(name, context, null);

        this.includeData = context.snapshotMode().includeData();
        this.snapshotLockingMode = context.getConnectorConfig().getSnapshotLockingMode();
        recorder = this::recordRowAsRead;
        metrics = new SnapshotReaderMetrics(context, context.dbSchema(), changeEventQueueMetrics);
        this.useGlobalLock = useGlobalLock;
    }

    /**
     * Set this reader's {@link #execute() execution} to produce a {@link io.debezium.data.Envelope.Operation#READ} event for each
     * row.
     *
     * @return this object for method chaining; never null
     */
    public SnapshotReader generateReadEvents() {
        recorder = this::recordRowAsRead;
        return this;
    }

    @Override
    protected void doInitialize() {
        metrics.register(logger);
    }

    @Override
    public void doDestroy() {
        metrics.unregister(logger);
    }

    /**
     * Start the snapshot and return immediately. Once started, the records read from the database can be retrieved using
     * {@link #poll()} until that method returns {@code null}.
     */
    @Override
    protected void doStart() {
        executorService = Threads.newSingleThreadExecutor(MySqlConnector.class, context.getConnectorConfig().getLogicalName(), "snapshot");
        executorService.execute(this::execute);
    }

    @Override
    protected void doStop() {
        logger.debug("Stopping snapshot reader");
        cleanupResources();
        // The parent class will change the isRunning() state, and this class' execute() uses that and will stop automatically
    }

    @Override
    protected void doCleanup() {
        executorService.shutdown();
        logger.debug("Completed writing all snapshot records");
    }

    protected Object readField(ResultSet rs, int fieldNo, Column actualColumn, Table actualTable) throws SQLException {
        if (actualColumn.jdbcType() == Types.TIME) {
            return readTimeField(rs, fieldNo);
        }
        else if (actualColumn.jdbcType() == Types.DATE) {
            return readDateField(rs, fieldNo, actualColumn, actualTable);
        }
        // This is for DATETIME columns (a logical date + time without time zone)
        // by reading them with a calendar based on the default time zone, we make sure that the value
        // is constructed correctly using the database's (or connection's) time zone
        else if (actualColumn.jdbcType() == Types.TIMESTAMP) {
            return readTimestampField(rs, fieldNo, actualColumn, actualTable);
        }
        // JDBC's rs.GetObject() will return a Boolean for all TINYINT(1) columns.
        // TINYINT columns are reprtoed as SMALLINT by JDBC driver
        else if (actualColumn.jdbcType() == Types.TINYINT || actualColumn.jdbcType() == Types.SMALLINT) {
            // It seems that rs.wasNull() returns false when default value is set and NULL is inserted
            // We thus need to use getObject() to identify if the value was provided and if yes then
            // read it again to get correct scale
            return rs.getObject(fieldNo) == null ? null : rs.getInt(fieldNo);
        }
        // DBZ-2673
        // It is necessary to check the type names as types like ENUM and SET are
        // also reported as JDBC type char
        else if ("CHAR".equals(actualColumn.typeName()) ||
                "VARCHAR".equals(actualColumn.typeName()) ||
                "TEXT".equals(actualColumn.typeName())) {
            return rs.getBytes(fieldNo);
        }
        else {
            return rs.getObject(fieldNo);
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
            logger.error("Could not read MySQL TIME value as UTF-8");
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
            logger.error("Could not read MySQL TIME value as UTF-8");
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
            logger.error("Could not read MySQL TIME value as UTF-8");
            throw new RuntimeException(e);
        }
    }

    /**
     * Perform the snapshot using the same logic as the "mysqldump" utility.
     */
    protected void execute() {
        context.configureLoggingContext("snapshot");
        final AtomicReference<String> sql = new AtomicReference<>();
        final JdbcConnection mysql = connectionContext.jdbc();
        final MySqlSchema schema = context.dbSchema();
        final Filters filters = schema.filters();
        final SourceInfo source = context.source();
        final Clock clock = context.getClock();
        final long ts = clock.currentTimeInMillis();
        logger.info("Starting snapshot for {} with user '{}' with locking mode '{}'", connectionContext.connectionString(), mysql.username(),
                snapshotLockingMode.getValue());
        logRolesForCurrentUser(mysql);
        logServerInformation(mysql);
        boolean isLocked = false;
        boolean isTxnStarted = false;
        boolean tableLocks = false;
        final List<TableId> tablesToSnapshotSchemaAfterUnlock = new ArrayList<>();
        Set<TableId> lockedTables = Collections.emptySet();

        final Set<String> snapshotAllowedTables = context.getConnectorConfig().getDataCollectionsToBeSnapshotted();
        final Predicate<TableId> isAllowedForSnapshot = tableId -> snapshotAllowedTables.size() == 0
                || snapshotAllowedTables.stream().anyMatch(s -> tableId.identifier().matches(s));
        try {
            metrics.snapshotStarted();

            // ------
            // STEP 0
            // ------
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
            if (!isRunning()) {
                return;
            }

            final long snapshotLockTimeout = context.getConnectorConfig().snapshotLockTimeout().getSeconds();
            logger.info("Step 0: disabling autocommit, enabling repeatable read transactions, and setting lock wait timeout to {}",
                    snapshotLockTimeout);
            mysql.setAutoCommit(false);
            sql.set("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            mysql.executeWithoutCommitting(sql.get());
            sql.set("SET SESSION lock_wait_timeout=" + snapshotLockTimeout);
            mysql.executeWithoutCommitting(sql.get());
            try {
                sql.set("SET SESSION innodb_lock_wait_timeout=" + snapshotLockTimeout);
                mysql.executeWithoutCommitting(sql.get());
            }
            catch (SQLException e) {
                logger.warn("Unable to set innodb_lock_wait_timeout", e);
            }

            // Generate the DDL statements that set the charset-related system variables ...
            Map<String, String> systemVariables = connectionContext.readMySqlCharsetSystemVariables();
            String setSystemVariablesStatement = connectionContext.setStatementFor(systemVariables);
            AtomicBoolean interrupted = new AtomicBoolean(false);
            long lockAcquired = 0L;
            int step = 1;

            Configuration configuration = context.config();
            try {
                // ------------------------------------
                // LOCK TABLES
                // ------------------------------------
                // Obtain read lock on all tables. This statement closes all open tables and locks all tables
                // for all databases with a global read lock, and it prevents ALL updates while we have this lock.
                // It also ensures that everything we do while we have this lock will be consistent.
                if (!isRunning()) {
                    return;
                }
                if (!snapshotLockingMode.equals(MySqlConnectorConfig.SnapshotLockingMode.NONE) && useGlobalLock) {
                    try {
                        logger.info("Step 1: flush and obtain global read lock to prevent writes to database");
                        sql.set(snapshotLockingMode.getLockStatement());
                        mysql.executeWithoutCommitting(sql.get());
                        lockAcquired = clock.currentTimeInMillis();
                        metrics.globalLockAcquired();
                        isLocked = true;
                    }
                    catch (SQLException e) {
                        logger.info("Step 1: unable to flush and acquire global read lock, will use table read locks after reading table names");
                        // Continue anyway, since RDS (among others) don't allow setting a global lock
                        assert !isLocked;
                    }
                    // FLUSH TABLES resets TX and isolation level
                    sql.set("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
                    mysql.executeWithoutCommitting(sql.get());
                }

                // ------
                // START TRANSACTION
                // ------
                // First, start a transaction and request that a consistent MVCC snapshot is obtained immediately.
                // See http://dev.mysql.com/doc/refman/5.7/en/commit.html
                if (!isRunning()) {
                    return;
                }
                logger.info("Step 2: start transaction with consistent snapshot");
                sql.set("START TRANSACTION WITH CONSISTENT SNAPSHOT");
                mysql.executeWithoutCommitting(sql.get());
                isTxnStarted = true;

                // ------------------------------------
                // READ BINLOG POSITION
                // ------------------------------------
                if (!isRunning()) {
                    return;
                }
                step = 3;
                if (isLocked) {
                    // Obtain the binlog position and update the SourceInfo in the context. This means that all source records
                    // generated as part of the snapshot will contain the binlog position of the snapshot.
                    readBinlogPosition(step++, source, mysql, sql);
                }

                // -------------------
                // READ DATABASE NAMES
                // -------------------
                // Get the list of databases ...
                if (!isRunning()) {
                    return;
                }
                logger.info("Step {}: read list of available databases", step++);
                final List<String> databaseNames = new ArrayList<>();
                sql.set("SHOW DATABASES");
                mysql.query(sql.get(), rs -> {
                    while (rs.next()) {
                        databaseNames.add(rs.getString(1));
                    }
                });
                logger.info("\t list of available databases is: {}", databaseNames);

                // ----------------
                // READ TABLE NAMES
                // ----------------
                // Get the list of table IDs for each database. We can't use a prepared statement with MySQL, so we have to
                // build the SQL statement each time. Although in other cases this might lead to SQL injection, in our case
                // we are reading the database names from the database and not taking them from the user ...
                if (!isRunning()) {
                    return;
                }
                logger.info("Step {}: read list of available tables in each database", step++);
                List<TableId> knownTableIds = new ArrayList<>();
                final List<TableId> capturedTableIds = new ArrayList<>();
                final Filters createTableFilters = getCreateTableFilters(filters);
                final Map<String, List<TableId>> createTablesMap = new HashMap<>();
                final Set<String> readableDatabaseNames = new HashSet<>();
                for (String dbName : databaseNames) {
                    try {
                        // MySQL sometimes considers some local files as databases (see DBZ-164),
                        // so we will simply try each one and ignore the problematic ones ...
                        sql.set("SHOW FULL TABLES IN " + quote(dbName) + " where Table_Type = 'BASE TABLE'");
                        mysql.query(sql.get(), rs -> {
                            while (rs.next() && isRunning()) {
                                TableId id = new TableId(dbName, null, rs.getString(1));
                                final boolean shouldRecordTableSchema = shouldRecordTableSchema(schema, filters, id);
                                // Apply only when the table include list is not dynamically reconfigured
                                if ((createTableFilters == filters && shouldRecordTableSchema) || createTableFilters.tableFilter().test(id)) {
                                    createTablesMap.computeIfAbsent(dbName, k -> new ArrayList<>()).add(id);
                                }
                                if (shouldRecordTableSchema) {
                                    knownTableIds.add(id);
                                    logger.info("\t including '{}' among known tables", id);
                                }
                                else {
                                    logger.info("\t '{}' is not added among known tables", id);
                                }
                                if (filters.tableFilter().and(isAllowedForSnapshot).test(id)) {
                                    capturedTableIds.add(id);
                                    logger.info("\t including '{}' for further processing", id);
                                }
                                else {
                                    logger.info("\t '{}' is filtered out of capturing", id);
                                }
                            }
                        });
                        readableDatabaseNames.add(dbName);
                    }
                    catch (SQLException e) {
                        // We were unable to execute the query or process the results, so skip this ...
                        logger.warn("\t skipping database '{}' due to error reading tables: {}", dbName, e.getMessage());
                    }
                }
                /*
                 * To achieve an ordered snapshot, we would first get a list of Regex tables.whitelist regex patterns
                 * + and then sort the tableIds list based on the above list
                 * +
                 */
                List<Pattern> tableIncludeListPattern = Strings.listOfRegex(
                        configuration.getFallbackStringProperty(MySqlConnectorConfig.TABLE_INCLUDE_LIST, MySqlConnectorConfig.TABLE_WHITELIST),
                        Pattern.CASE_INSENSITIVE);
                List<TableId> tableIdsSorted = new ArrayList<>();
                tableIncludeListPattern.forEach(pattern -> {
                    List<TableId> tablesMatchedByPattern = capturedTableIds.stream().filter(t -> pattern.asPredicate().test(t.toString()))
                            .collect(Collectors.toList());
                    tablesMatchedByPattern.forEach(t -> {
                        if (!tableIdsSorted.contains(t)) {
                            tableIdsSorted.add(t);
                        }
                    });
                });
                capturedTableIds.sort(Comparator.comparing(tableIdsSorted::indexOf));
                final Set<String> includedDatabaseNames = readableDatabaseNames.stream().filter(filters.databaseFilter()).collect(Collectors.toSet());
                logger.info("\tsnapshot continuing with database(s): {}", includedDatabaseNames);

                if (!isLocked) {
                    if (!snapshotLockingMode.equals(MySqlConnectorConfig.SnapshotLockingMode.NONE)) {
                        // ------------------------------------
                        // LOCK TABLES and READ BINLOG POSITION
                        // ------------------------------------
                        // We were not able to acquire the global read lock, so instead we have to obtain a read lock on each table.
                        // This requires different privileges than normal, and also means we can't unlock the tables without
                        // implicitly committing our transaction ...
                        if (!connectionContext.userHasPrivileges("LOCK TABLES")) {
                            // We don't have the right privileges
                            throw new ConnectException("User does not have the 'LOCK TABLES' privilege required to obtain a "
                                    + "consistent snapshot by preventing concurrent writes to tables.");
                        }
                        // We have the required privileges, so try to lock all of the tables we're interested in ...
                        logger.info("Step {}: flush and obtain read lock for {} tables (preventing writes)", step++, knownTableIds.size());
                        lockedTables = new HashSet<>(capturedTableIds);
                        String tableList = capturedTableIds.stream()
                                .map(tid -> quote(tid))
                                .reduce((r, element) -> r + "," + element)
                                .orElse(null);
                        if (tableList != null) {
                            sql.set("FLUSH TABLES " + tableList + " WITH READ LOCK");
                            mysql.executeWithoutCommitting(sql.get());
                        }
                        lockAcquired = clock.currentTimeInMillis();
                        metrics.globalLockAcquired();
                        isLocked = true;
                        tableLocks = true;
                    }

                    // Our tables are locked, so read the binlog position ...
                    readBinlogPosition(step++, source, mysql, sql);
                }

                // From this point forward, all source records produced by this connector will have an offset that includes a
                // "snapshot" field (with value of "true").

                // ------
                // STEP 6
                // ------
                // Transform the current schema so that it reflects the *current* state of the MySQL server's contents.
                // First, get the DROP TABLE and CREATE TABLE statement (with keys and constraint definitions) for our tables ...

                try {
                    logger.info("Step {}: generating DROP and CREATE statements to reflect current database schemas:", step++);
                    schema.applyDdl(source, null, setSystemVariablesStatement, this::enqueueSchemaChanges);

                    // Add DROP TABLE statements for all tables that we knew about AND those tables found in the databases ...
                    knownTableIds.stream()
                            .filter(id -> isRunning()) // ignore all subsequent tables if this reader is stopped
                            .forEach(tableId -> schema.applyDdl(source, tableId.catalog(),
                                    "DROP TABLE IF EXISTS " + quote(tableId),
                                    this::enqueueSchemaChanges));

                    // Add a DROP DATABASE statement for each database that we no longer know about ...
                    schema.tableIds().stream().map(TableId::catalog)
                            .filter(Predicates.not(readableDatabaseNames::contains))
                            .filter(id -> isRunning()) // ignore all subsequent tables if this reader is stopped
                            .forEach(missingDbName -> schema.applyDdl(source, missingDbName,
                                    "DROP DATABASE IF EXISTS " + quote(missingDbName),
                                    this::enqueueSchemaChanges));

                    final Map<String, DatabaseLocales> databaseCharsets = connectionContext.readDatabaseCollations();
                    // Now process all of our tables for each database ...
                    for (Map.Entry<String, List<TableId>> entry : createTablesMap.entrySet()) {
                        if (!isRunning()) {
                            break;
                        }
                        String dbName = entry.getKey();
                        // First drop, create, and then use the named database ...
                        schema.applyDdl(source, dbName, "DROP DATABASE IF EXISTS " + quote(dbName), this::enqueueSchemaChanges);

                        final StringBuilder createDatabaseDddl = new StringBuilder("CREATE DATABASE " + quote(dbName));
                        final DatabaseLocales defaultDatabaseLocales = databaseCharsets.get(dbName);
                        if (defaultDatabaseLocales != null) {
                            defaultDatabaseLocales.appendToDdlStatement(dbName, createDatabaseDddl);
                        }
                        schema.applyDdl(source, dbName, createDatabaseDddl.toString(), this::enqueueSchemaChanges);

                        schema.applyDdl(source, dbName, "USE " + quote(dbName), this::enqueueSchemaChanges);
                        for (TableId tableId : entry.getValue()) {
                            if (!isRunning()) {
                                break;
                            }
                            // This is to handle situation when global read lock is unavailable and tables are locked instead of it.
                            // MySQL forbids access to an unlocked table when there is at least one lock held on another table.
                            // Thus when we need to obtain schema even for non-monitored tables (which are not locked as we might not have access privileges)
                            // we need to do it after the tables are unlocked
                            if (lockedTables.isEmpty() || lockedTables.contains(tableId)) {
                                readTableSchema(sql, mysql, schema, source, dbName, tableId);
                            }
                            else {
                                tablesToSnapshotSchemaAfterUnlock.add(tableId);
                            }
                        }
                    }
                    context.makeRecord().regenerate();
                }
                // most likely, something went wrong while writing the history topic
                catch (Exception e) {
                    interrupted.set(true);
                    throw e;
                }

                // ------
                // STEP 7
                // ------
                if (snapshotLockingMode.usesMinimalLocking() && isLocked) {
                    if (tableLocks) {
                        // We could not acquire a global read lock and instead had to obtain individual table-level read locks
                        // using 'FLUSH TABLE <tableName> WITH READ LOCK'. However, if we were to do this, the 'UNLOCK TABLES'
                        // would implicitly commit our active transaction, and this would break our consistent snapshot logic.
                        // Therefore, we cannot unlock the tables here!
                        // https://dev.mysql.com/doc/refman/5.7/en/flush.html
                        logger.info("Step {}: tables were locked explicitly, but to get a consistent snapshot we cannot "
                                + "release the locks until we've read all tables.", step++);
                    }
                    else {
                        // We are doing minimal blocking via a global read lock, so we should release the global read lock now.
                        // All subsequent SELECT should still use the MVCC snapshot obtained when we started our transaction
                        // (since we started it "...with consistent snapshot"). So, since we're only doing very simple SELECT
                        // without WHERE predicates, we can release the lock now ...
                        logger.info("Step {}: releasing global read lock to enable MySQL writes", step);
                        sql.set("UNLOCK TABLES");
                        mysql.executeWithoutCommitting(sql.get());
                        isLocked = false;
                        long lockReleased = clock.currentTimeInMillis();
                        metrics.globalLockReleased();
                        logger.info("Step {}: blocked writes to MySQL for a total of {}", step++,
                                Strings.duration(lockReleased - lockAcquired));
                    }
                }

                // ------
                // STEP 8
                // ------
                // Use a buffered blocking consumer to buffer all of the records, so that after we copy all of the tables
                // and produce events we can update the very last event with the non-snapshot offset ...
                if (!isRunning()) {
                    return;
                }
                if (includeData) {
                    BufferedBlockingConsumer<SourceRecord> bufferedRecordQueue = BufferedBlockingConsumer.bufferLast(super::enqueueRecord);

                    // Dump all of the tables and generate source records ...
                    logger.info("Step {}: scanning contents of {} tables while still in transaction", step, capturedTableIds.size());
                    metrics.monitoredDataCollectionsDetermined(capturedTableIds);

                    long startScan = clock.currentTimeInMillis();
                    AtomicLong totalRowCount = new AtomicLong();
                    int counter = 0;
                    int completedCounter = 0;
                    long largeTableCount = context.rowCountForLargeTable();
                    Iterator<TableId> tableIdIter = capturedTableIds.iterator();
                    while (tableIdIter.hasNext()) {
                        TableId tableId = tableIdIter.next();
                        AtomicLong rowNum = new AtomicLong();
                        if (!isRunning()) {
                            break;
                        }

                        // Obtain a record maker for this table, which knows about the schema ...
                        RecordsForTable recordMaker = context.makeRecord().forTable(tableId, null, bufferedRecordQueue);
                        if (recordMaker != null) {

                            // Switch to the table's database ...
                            sql.set("USE " + quote(tableId.catalog()) + ";");
                            mysql.executeWithoutCommitting(sql.get());

                            AtomicLong numRows = new AtomicLong(-1);
                            AtomicReference<String> rowCountStr = new AtomicReference<>("<unknown>");
                            StatementFactory statementFactory = this::createStatementWithLargeResultSet;
                            if (largeTableCount > 0) {
                                try {
                                    // Choose how we create statements based on the # of rows.
                                    // This is approximate and less accurate then COUNT(*),
                                    // but far more efficient for large InnoDB tables.
                                    sql.set("SHOW TABLE STATUS LIKE '" + tableId.table() + "';");
                                    mysql.query(sql.get(), rs -> {
                                        if (rs.next()) {
                                            numRows.set(rs.getLong(5));
                                        }
                                    });
                                    if (numRows.get() <= largeTableCount) {
                                        statementFactory = this::createStatement;
                                    }
                                    rowCountStr.set(numRows.toString());
                                }
                                catch (SQLException e) {
                                    // Log it, but otherwise just use large result set by default ...
                                    logger.debug("Error while getting number of rows in table {}: {}", tableId, e.getMessage(), e);
                                }
                            }

                            // Scan the rows in the table ...
                            long start = clock.currentTimeInMillis();
                            logger.info("Step {}: - scanning table '{}' ({} of {} tables)", step, tableId, ++counter, capturedTableIds.size());

                            Map<TableId, String> selectOverrides = context.getConnectorConfig().getSnapshotSelectOverridesByTable();

                            String selectStatement = selectOverrides.getOrDefault(tableId, "SELECT * FROM " + quote(tableId));
                            logger.info("For table '{}' using select statement: '{}'", tableId, selectStatement);
                            sql.set(selectStatement);

                            try {
                                int stepNum = step;
                                mysql.query(sql.get(), statementFactory, rs -> {
                                    try {
                                        // The table is included in the connector's filters, so process all of the table records
                                        // ...
                                        final Table table = schema.tableFor(tableId);
                                        final int numColumns = table.columns().size();
                                        final Object[] row = new Object[numColumns];
                                        while (rs.next()) {
                                            for (int i = 0, j = 1; i != numColumns; ++i, ++j) {
                                                Column actualColumn = table.columns().get(i);
                                                row[i] = readField(rs, j, actualColumn, table);
                                            }
                                            recorder.recordRow(recordMaker, row, clock.currentTimeAsInstant()); // has no row number!
                                            rowNum.incrementAndGet();
                                            if (rowNum.get() % 100 == 0 && !isRunning()) {
                                                // We've stopped running ...
                                                break;
                                            }
                                            if (rowNum.get() % 10_000 == 0) {
                                                if (logger.isInfoEnabled()) {
                                                    long stop = clock.currentTimeInMillis();
                                                    logger.info("Step {}: - {} of {} rows scanned from table '{}' after {}",
                                                            stepNum, rowNum, rowCountStr, tableId, Strings.duration(stop - start));
                                                }
                                                metrics.rowsScanned(tableId, rowNum.get());
                                            }
                                        }
                                        totalRowCount.addAndGet(rowNum.get());
                                        if (isRunning()) {
                                            if (logger.isInfoEnabled()) {
                                                long stop = clock.currentTimeInMillis();
                                                logger.info("Step {}: - Completed scanning a total of {} rows from table '{}' after {}",
                                                        stepNum, rowNum, tableId, Strings.duration(stop - start));
                                            }
                                            metrics.rowsScanned(tableId, rowNum.get());
                                        }
                                    }
                                    catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                        // We were not able to finish all rows in all tables ...
                                        logger.info("Step {}: Stopping the snapshot due to thread interruption", stepNum);
                                        interrupted.set(true);
                                    }
                                });
                            }
                            finally {
                                metrics.dataCollectionSnapshotCompleted(tableId, rowNum.get());
                                if (interrupted.get()) {
                                    break;
                                }
                            }
                        }
                        ++completedCounter;
                    }

                    // See if we've been stopped or interrupted ...
                    if (!isRunning() || interrupted.get()) {
                        return;
                    }

                    // We've copied all of the tables and we've not yet been stopped, but our buffer holds onto the
                    // very last record. First mark the snapshot as complete and then apply the updated offset to
                    // the buffered record ...
                    source.markLastSnapshot(configuration);
                    long stop = clock.currentTimeInMillis();
                    try {
                        bufferedRecordQueue.close(this::replaceOffsetAndSource);
                        if (logger.isInfoEnabled()) {
                            logger.info("Step {}: scanned {} rows in {} tables in {}",
                                    step, totalRowCount, capturedTableIds.size(), Strings.duration(stop - startScan));
                        }
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        // We were not able to finish all rows in all tables ...
                        if (logger.isInfoEnabled()) {
                            logger.info("Step {}: aborting the snapshot after {} rows in {} of {} tables {}",
                                    step, totalRowCount, completedCounter, capturedTableIds.size(), Strings.duration(stop - startScan));
                        }
                        interrupted.set(true);
                    }
                }
                else {
                    logger.info("Step {}: encountered only schema based snapshot, skipping data snapshot", step);
                }
                step++;
            }
            finally {
                // No matter what, we always want to do these steps if necessary ...
                boolean rolledBack = false;
                // ------
                // STEP 9
                // ------
                // Either commit or roll back the transaction, BEFORE releasing the locks ...
                if (isTxnStarted) {
                    if (interrupted.get() || !isRunning()) {
                        // We were interrupted or were stopped while reading the tables,
                        // so roll back the transaction and return immediately ...
                        logger.info("Step {}: rolling back transaction after abort", step++);
                        mysql.connection().rollback();
                        metrics.snapshotAborted();
                        rolledBack = true;
                    }
                    else {
                        // Otherwise, commit our transaction
                        logger.info("Step {}: committing transaction", step++);
                        mysql.connection().commit();
                        metrics.snapshotCompleted();
                    }
                }
                else {
                    // Always clean up TX resources even if no changes might be done
                    mysql.connection().rollback();
                }

                // -------
                // STEP 10
                // -------
                // Release the read lock(s) if we have not yet done so. Locks are not released when committing/rolling back ...
                if (isLocked && !rolledBack) {
                    if (tableLocks) {
                        logger.info("Step {}: releasing table read locks to enable MySQL writes", step++);
                    }
                    else {
                        logger.info("Step {}: releasing global read lock to enable MySQL writes", step++);
                    }
                    sql.set("UNLOCK TABLES");
                    mysql.executeWithoutCommitting(sql.get());
                    isLocked = false;
                    long lockReleased = clock.currentTimeInMillis();
                    metrics.globalLockReleased();
                    if (logger.isInfoEnabled()) {
                        if (tableLocks) {
                            logger.info("Writes to MySQL prevented for a total of {}", Strings.duration(lockReleased - lockAcquired));
                        }
                        else {
                            logger.info("Writes to MySQL tables prevented for a total of {}", Strings.duration(lockReleased - lockAcquired));
                        }
                    }
                    if (!tablesToSnapshotSchemaAfterUnlock.isEmpty()) {
                        logger.info("Step {}: reading table schema for non-whitelisted tables", step++);
                        for (TableId tableId : tablesToSnapshotSchemaAfterUnlock) {
                            if (!isRunning()) {
                                break;
                            }
                            readTableSchema(sql, mysql, schema, source, tableId.catalog(), tableId);
                        }
                    }
                }
            }

            if (!isRunning()) {
                // The reader (and connector) was stopped and we did not finish ...
                try {
                    // Mark this reader as having completing its work ...
                    completeSuccessfully();
                    if (logger.isInfoEnabled()) {
                        long stop = clock.currentTimeInMillis();
                        logger.info("Stopped snapshot after {} but before completing", Strings.duration(stop - ts));
                    }
                }
                finally {
                    // and since there's no more work to do clean up all resources ...
                    cleanupResources();
                }
            }
            else {
                // We completed the snapshot...
                try {
                    // Mark the source as having completed the snapshot. This will ensure the `source` field on records
                    // are not denoted as a snapshot ...
                    source.completeSnapshot();
                    Heartbeat
                            .create(
                                    configuration.getDuration(Heartbeat.HEARTBEAT_INTERVAL, ChronoUnit.MILLIS),
                                    context.topicSelector().getHeartbeatTopic(),
                                    context.getConnectorConfig().getLogicalName())
                            .forcedBeat(source.partition(), source.offset(), this::enqueueRecord);
                }
                finally {
                    // Set the completion flag ...
                    completeSuccessfully();
                    if (logger.isInfoEnabled()) {
                        long stop = clock.currentTimeInMillis();
                        logger.info("Completed snapshot in {}", Strings.duration(stop - ts));
                    }
                }
            }
        }
        catch (Throwable e) {
            failed(e, "Aborting snapshot due to error when last running '" + sql.get() + "': " + e.getMessage());
            if (isLocked) {
                try {
                    sql.set("UNLOCK TABLES");
                    mysql.executeWithoutCommitting(sql.get());
                }
                catch (Exception eUnlock) {
                    logger.error("Removing of table locks not completed successfully", eUnlock);
                }
                try {
                    mysql.connection().rollback();
                }
                catch (Exception eRollback) {
                    logger.error("Execption while rollback is executed", eRollback);
                }
            }
        }
        finally {
            try {
                mysql.close();
            }
            catch (SQLException e) {
                logger.warn("Failed to close the connection properly", e);
            }
        }
    }

    private void readTableSchema(final AtomicReference<String> sql, final JdbcConnection mysql,
                                 final MySqlSchema schema, final SourceInfo source, String dbName, TableId tableId)
            throws SQLException {
        sql.set("SHOW CREATE TABLE " + quote(tableId));
        mysql.query(sql.get(), rs -> {
            if (rs.next()) {
                schema.applyDdl(source, dbName, rs.getString(2), this::enqueueSchemaChanges);
            }
        });
    }

    /**
     * Whether DDL for the given table should be recorded.
     */
    private boolean shouldRecordTableSchema(MySqlSchema schema, Filters filters, TableId id) {
        // some tables are always ignored, also if we're recording the schema of non-captured tables
        if (filters.ignoredTableFilter().test(id)) {
            return false;
        }

        return filters.tableFilter().test(id) || !schema.isStoreOnlyMonitoredTablesDdl();
    }

    protected void readBinlogPosition(int step, SourceInfo source, JdbcConnection mysql, AtomicReference<String> sql) throws SQLException {
        if (context.isSchemaOnlyRecoverySnapshot()) {
            // We are in schema only recovery mode, use the existing binlog position
            if (Strings.isNullOrEmpty(source.binlogFilename())) {
                // would like to also verify binlog position exists, but it defaults to 0 which is technically valid
                throw new IllegalStateException("Could not find existing binlog information while attempting schema only recovery snapshot");
            }
            source.startSnapshot();
        }
        else {
            logger.info("Step {}: read binlog position of MySQL primary server", step);
            String showMasterStmt = "SHOW MASTER STATUS";
            sql.set(showMasterStmt);
            mysql.query(sql.get(), rs -> {
                if (rs.next()) {
                    String binlogFilename = rs.getString(1);
                    long binlogPosition = rs.getLong(2);
                    source.setBinlogStartPoint(binlogFilename, binlogPosition);
                    if (rs.getMetaData().getColumnCount() > 4) {
                        // This column exists only in MySQL 5.6.5 or later ...
                        String gtidSet = rs.getString(5); // GTID set, may be null, blank, or contain a GTID set
                        source.setCompletedGtidSet(gtidSet);
                        logger.info("\t using binlog '{}' at position '{}' and gtid '{}'", binlogFilename, binlogPosition,
                                gtidSet);
                    }
                    else {
                        logger.info("\t using binlog '{}' at position '{}'", binlogFilename, binlogPosition);
                    }
                    source.startSnapshot();
                }
                else {
                    throw new IllegalStateException("Cannot read the binlog filename and position via '" + showMasterStmt
                            + "'. Make sure your server is correctly configured");
                }
            });
        }
    }

    /**
     * Get the filters for table creation. Depending on the configuration, this may not be the default filter set.
     *
     * @param filters the default filters of this {@link SnapshotReader}
     * @return {@link Filters} that represent all the tables that this snapshot reader should CREATE
     */
    private Filters getCreateTableFilters(Filters filters) {
        MySqlConnectorConfig.SnapshotNewTables snapshotNewTables = context.getConnectorConfig().getSnapshotNewTables();
        if (snapshotNewTables == MySqlConnectorConfig.SnapshotNewTables.PARALLEL) {
            // if we are snapshotting new tables in parallel, we need to make sure all the tables in the configuration
            // are created.
            return new Filters.Builder(context.config()).build();
        }
        else {
            return filters;
        }
    }

    protected String quote(String dbOrTableName) {
        return "`" + dbOrTableName + "`";
    }

    protected String quote(TableId id) {
        return quote(id.catalog()) + "." + quote(id.table());
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
     * @param connection the JDBC connection; may not be null
     * @return the statement; never null
     * @throws SQLException if there is a problem creating the statement
     */
    private Statement createStatementWithLargeResultSet(Connection connection) throws SQLException {
        int fetchSize = context.getConnectorConfig().getSnapshotFetchSize();
        Statement stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(fetchSize);
        return stmt;
    }

    private Statement createStatement(Connection connection) throws SQLException {
        return connection.createStatement();
    }

    private void logServerInformation(JdbcConnection mysql) {
        try {
            logger.info("MySQL server variables related to change data capture:");
            mysql.query("SHOW VARIABLES WHERE Variable_name REGEXP 'version|binlog|tx_|gtid|character_set|collation|time_zone'", rs -> {
                while (rs.next()) {
                    logger.info("\t{} = {}",
                            Strings.pad(rs.getString(1), 45, ' '),
                            Strings.pad(rs.getString(2), 45, ' '));
                }
            });
        }
        catch (SQLException e) {
            logger.info("Cannot determine MySql server version", e);
        }
    }

    private void logRolesForCurrentUser(JdbcConnection mysql) {
        try {
            List<String> grants = new ArrayList<>();
            mysql.query("SHOW GRANTS FOR CURRENT_USER", rs -> {
                while (rs.next()) {
                    grants.add(rs.getString(1));
                }
            });
            if (grants.isEmpty()) {
                logger.warn("Snapshot is using user '{}' but it likely doesn't have proper privileges. " +
                        "If tables are missing or are empty, ensure connector is configured with the correct MySQL user " +
                        "and/or ensure that the MySQL user has the required privileges.",
                        mysql.username());
            }
            else {
                logger.info("Snapshot is using user '{}' with these MySQL grants:", mysql.username());
                grants.forEach(grant -> logger.info("\t{}", grant));
            }
        }
        catch (SQLException e) {
            logger.info("Cannot determine the privileges for '{}' ", mysql.username(), e);
        }
    }

    /**
     * Utility method to replace the offset and the source in the given record with the latest. This is used on the last record produced
     * during the snapshot.
     *
     * @param record the record
     * @return the updated record
     */
    protected SourceRecord replaceOffsetAndSource(SourceRecord record) {
        if (record == null) {
            return null;
        }
        Map<String, ?> newOffset = context.source().offset();
        final Struct envelope = (Struct) record.value();
        final Struct source = (Struct) envelope.get(Envelope.FieldName.SOURCE);
        if (SnapshotRecord.fromSource(source) == SnapshotRecord.TRUE) {
            SnapshotRecord.LAST.toSource(source);
        }
        return new SourceRecord(record.sourcePartition(),
                newOffset,
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                record.value());
    }

    protected void enqueueSchemaChanges(String dbName, Set<TableId> tables, String ddlStatement) {
        if (!context.includeSchemaChangeRecords() || ddlStatement.length() == 0) {
            return;
        }
        if (context.makeRecord().schemaChanges(dbName, tables, ddlStatement, super::enqueueRecord) > 0) {
            logger.info("\t{}", ddlStatement);
        }
    }

    protected void recordRowAsRead(RecordsForTable recordMaker, Object[] row, Instant ts) throws InterruptedException {
        recordMaker.read(row, ts);
    }

    protected void recordRowAsInsert(RecordsForTable recordMaker, Object[] row, Instant ts) throws InterruptedException {
        recordMaker.create(row, ts);
    }

    protected static interface RecordRecorder {
        void recordRow(RecordsForTable recordMaker, Object[] row, Instant ts) throws InterruptedException;
    }
}
