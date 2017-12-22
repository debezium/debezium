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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.mysql.RecordMakers.RecordsForTable;
import io.debezium.function.BufferedBlockingConsumer;
import io.debezium.function.Predicates;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcConnection.StatementFactory;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

/**
 * A component that performs a snapshot of a MySQL server, and records the schema changes in {@link MySqlSchema}.
 *
 * @author Randall Hauch
 */
public class SnapshotReader extends AbstractReader {

    /**
     * Used to parse values of TIME columns. Format: 000:00:00.000000.
     */
    private static final Pattern TIME_FIELD_PATTERN = Pattern.compile("(\\-?[0-9]*):([0-9]*):([0-9]*)(\\.([0-9]*))?");

    private boolean minimalBlocking = true;
    private final boolean includeData;
    private RecordRecorder recorder;
    private volatile Thread thread;
    private final SnapshotReaderMetrics metrics;

    /**
     * Create a snapshot reader.
     *
     * @param name the name of this reader; may not be null
     * @param context the task context in which this reader is running; may not be null
     */
    public SnapshotReader(String name, MySqlTaskContext context) {
        super(name, context);
        this.includeData = !context.isSchemaOnlySnapshot();
        recorder = this::recordRowAsRead;
        metrics = new SnapshotReaderMetrics(context.clock());
    }

    /**
     * Set whether this reader's {@link #execute() execution} should block other transactions as minimally as possible by
     * releasing the read lock as early as possible. Although the snapshot process should obtain a consistent snapshot even
     * when releasing the lock as early as possible, it may be desirable to explicitly hold onto the read lock until execution
     * completes. In such cases, holding onto the lock will prevent all updates to the database during the snapshot process.
     *
     * @param minimalBlocking {@code true} if the lock is to be released as early as possible, or {@code false} if the lock
     *            is to be held for the entire {@link #execute() execution}
     * @return this object for method chaining; never null
     */
    public SnapshotReader useMinimalBlocking(boolean minimalBlocking) {
        this.minimalBlocking = minimalBlocking;
        return this;
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

    /**
     * Set this reader's {@link #execute() execution} to produce a {@link io.debezium.data.Envelope.Operation#CREATE} event for
     * each row.
     *
     * @return this object for method chaining; never null
     */
    public SnapshotReader generateInsertEvents() {
        recorder = this::recordRowAsInsert;
        return this;
    }

    @Override
    protected void doInitialize() {
        metrics.register(context, logger);
    }

    /**
     * Start the snapshot and return immediately. Once started, the records read from the database can be retrieved using
     * {@link #poll()} until that method returns {@code null}.
     */
    @Override
    protected void doStart() {
        thread = new Thread(this::execute, "mysql-snapshot-" + context.serverName());
        thread.start();
    }

    @Override
    protected void doStop() {
        logger.debug("Stopping snapshot reader");
        // The parent class will change the isRunning() state, and this class' execute() uses that and will stop automatically
    }

    @Override
    protected void doCleanup() {
        try {
            this.thread = null;
            logger.debug("Completed writing all snapshot records");
        } finally {
            metrics.unregister(logger);
        }
    }

    protected Object readField(ResultSet rs, int fieldNo, Column actualColumn) throws SQLException {
        if(actualColumn.jdbcType() == Types.TIME) {
            return readTimeField(rs, fieldNo);
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
        if (b == null) return null; // Don't continue parsing time field if it is null
        String timeString;

        try {
            timeString = new String(b.getBytes(1, (int) (b.length())), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            logger.error("Could not read MySQL TIME value as UTF-8");
            throw new RuntimeException(e);
        }

        Matcher matcher = TIME_FIELD_PATTERN.matcher(timeString);
        if (!matcher.matches()) {
            throw new RuntimeException("Unexpected format for TIME column: " + timeString);
        }

        long hours = Long.parseLong(matcher.group(1));
        long minutes = Long.parseLong(matcher.group(2));
        long seconds = Long.parseLong(matcher.group(3));
        long nanoSeconds = 0;
        String microSecondsString = matcher.group(5);
        if (microSecondsString != null) {
            nanoSeconds = Long.parseLong(rightPad(microSecondsString, 9, '0'));
        }

        if (hours >= 0) {
            return Duration.ofHours(hours)
                    .plusMinutes(minutes)
                    .plusSeconds(seconds)
                    .plusNanos(nanoSeconds);
        }
        else {
            return Duration.ofHours(hours)
                    .minusMinutes(minutes)
                    .minusSeconds(seconds)
                    .minusNanos(nanoSeconds);
        }
    }

    private String rightPad(String input, int length, char c) {
        char[] padded = new char[length];

        System.arraycopy(input.toCharArray(), 0, padded, 0, input.length());
        Arrays.fill(padded, input.length(), length, c);

        return new String(padded);
    }

    /**
     * Perform the snapshot using the same logic as the "mysqldump" utility.
     */
    protected void execute() {
        context.configureLoggingContext("snapshot");
        final AtomicReference<String> sql = new AtomicReference<>();
        final JdbcConnection mysql = context.jdbc();
        final MySqlSchema schema = context.dbSchema();
        final Filters filters = schema.filters();
        final SourceInfo source = context.source();
        final Clock clock = context.clock();
        final long ts = clock.currentTimeInMillis();
        logger.info("Starting snapshot for {} with user '{}'", context.connectionString(), mysql.username());
        logRolesForCurrentUser(mysql);
        logServerInformation(mysql);
        boolean isLocked = false;
        boolean isTxnStarted = false;
        boolean tableLocks = false;
        try {
            metrics.startSnapshot();

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
            if (!isRunning()) return;
            logger.info("Step 0: disabling autocommit and enabling repeatable read transactions");
            mysql.setAutoCommit(false);
            sql.set("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            mysql.execute(sql.get());

            // Generate the DDL statements that set the charset-related system variables ...
            Map<String, String> systemVariables = context.readMySqlCharsetSystemVariables(sql);
            String setSystemVariablesStatement = context.setStatementFor(systemVariables);
            AtomicBoolean interrupted = new AtomicBoolean(false);
            long lockAcquired = 0L;
            int step = 1;

            try {
                // ------------------------------------
                // LOCK TABLES
                // ------------------------------------
                // Obtain read lock on all tables. This statement closes all open tables and locks all tables
                // for all databases with a global read lock, and it prevents ALL updates while we have this lock.
                // It also ensures that everything we do while we have this lock will be consistent.
                if (!isRunning()) return;
                try {
                    logger.info("Step 1: flush and obtain global read lock to prevent writes to database");
                    sql.set("FLUSH TABLES WITH READ LOCK");
                    mysql.execute(sql.get());
                    lockAcquired = clock.currentTimeInMillis();
                    metrics.globalLockAcquired();
                    isLocked = true;
                } catch (SQLException e) {
                    logger.info("Step 1: unable to flush and acquire global read lock, will use table read locks after reading table names");
                    // Continue anyway, since RDS (among others) don't allow setting a global lock
                    assert !isLocked;
                }

                // ------
                // START TRANSACTION
                // ------
                // First, start a transaction and request that a consistent MVCC snapshot is obtained immediately.
                // See http://dev.mysql.com/doc/refman/5.7/en/commit.html
                if (!isRunning()) return;
                logger.info("Step 2: start transaction with consistent snapshot");
                sql.set("START TRANSACTION WITH CONSISTENT SNAPSHOT");
                mysql.execute(sql.get());
                isTxnStarted = true;

                // ------------------------------------
                // READ BINLOG POSITION
                // ------------------------------------
                if (!isRunning()) return;
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
                if (!isRunning()) return;
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
                if (!isRunning()) return;
                logger.info("Step {}: read list of available tables in each database", step++);
                List<TableId> tableIds = new ArrayList<>();
                final Map<String, List<TableId>> tableIdsByDbName = new HashMap<>();
                final Set<String> readableDatabaseNames = new HashSet<>();
                for (String dbName : databaseNames) {
                    try {
                        // MySQL sometimes considers some local files as databases (see DBZ-164),
                        // so we will simply try each one and ignore the problematic ones ...
                        sql.set("SHOW FULL TABLES IN " + quote(dbName) + " where Table_Type = 'BASE TABLE'");
                        mysql.query(sql.get(), rs -> {
                            while (rs.next() && isRunning()) {
                                TableId id = new TableId(dbName, null, rs.getString(1));
                                if (filters.tableFilter().test(id)) {
                                    tableIds.add(id);
                                    tableIdsByDbName.computeIfAbsent(dbName, k -> new ArrayList<>()).add(id);
                                    logger.info("\t including '{}'", id);
                                } else {
                                    logger.info("\t '{}' is filtered out, discarding", id);
                                }
                            }
                        });
                        readableDatabaseNames.add(dbName);
                    } catch (SQLException e) {
                        // We were unable to execute the query or process the results, so skip this ...
                        logger.warn("\t skipping database '{}' due to error reading tables: {}", dbName, e.getMessage());
                    }
                }
                final Set<String> includedDatabaseNames = readableDatabaseNames.stream().filter(filters.databaseFilter()).collect(Collectors.toSet());
                logger.info("\tsnapshot continuing with database(s): {}", includedDatabaseNames);

                if (!isLocked) {
                    // ------------------------------------
                    // LOCK TABLES and READ BINLOG POSITION
                    // ------------------------------------
                    // We were not able to acquire the global read lock, so instead we have to obtain a read lock on each table.
                    // This requires different privileges than normal, and also means we can't unlock the tables without
                    // implicitly committing our transaction ...
                    if (!context.userHasPrivileges("LOCK TABLES")) {
                        // We don't have the right privileges
                        throw new ConnectException("User does not have the 'LOCK TABLES' privilege required to obtain a "
                                + "consistent snapshot by preventing concurrent writes to tables.");
                    }
                    // We have the required privileges, so try to lock all of the tables we're interested in ...
                    logger.info("Step {}: flush and obtain read lock for {} tables (preventing writes)", step++, tableIds.size());
                    String tableList = tableIds.stream()
                            .map(tid -> quote(tid))
                            .reduce((r, element) -> r+ "," + element)
                            .orElse(null);
                    if (tableList != null) {
                        sql.set("FLUSH TABLES " + tableList + " WITH READ LOCK");
                        mysql.execute(sql.get());
                    }
                    lockAcquired = clock.currentTimeInMillis();
                    metrics.globalLockAcquired();
                    isLocked = true;
                    tableLocks = true;

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
                logger.info("Step {}: generating DROP and CREATE statements to reflect current database schemas:", step++);
                schema.applyDdl(source, null, setSystemVariablesStatement, this::enqueueSchemaChanges);

                // Add DROP TABLE statements for all tables that we knew about AND those tables found in the databases ...
                Set<TableId> allTableIds = new HashSet<>(schema.tables().tableIds());
                allTableIds.addAll(tableIds);
                allTableIds.stream()
                           .filter(id -> isRunning()) // ignore all subsequent tables if this reader is stopped
                           .forEach(tableId -> schema.applyDdl(source, tableId.schema(),
                                                               "DROP TABLE IF EXISTS " + quote(tableId),
                                                               this::enqueueSchemaChanges));

                // Add a DROP DATABASE statement for each database that we no longer know about ...
                schema.tables().tableIds().stream().map(TableId::catalog)
                      .filter(Predicates.not(readableDatabaseNames::contains))
                      .filter(id -> isRunning()) // ignore all subsequent tables if this reader is stopped
                      .forEach(missingDbName -> schema.applyDdl(source, missingDbName,
                                                                "DROP DATABASE IF EXISTS " + quote(missingDbName),
                                                                this::enqueueSchemaChanges));

                // Now process all of our tables for each database ...
                for (Map.Entry<String, List<TableId>> entry : tableIdsByDbName.entrySet()) {
                    if (!isRunning()) break;
                    String dbName = entry.getKey();
                    // First drop, create, and then use the named database ...
                    schema.applyDdl(source, dbName, "DROP DATABASE IF EXISTS " + quote(dbName), this::enqueueSchemaChanges);
                    schema.applyDdl(source, dbName, "CREATE DATABASE " + quote(dbName), this::enqueueSchemaChanges);
                    schema.applyDdl(source, dbName, "USE " + quote(dbName), this::enqueueSchemaChanges);
                    for (TableId tableId : entry.getValue()) {
                        if (!isRunning()) break;
                        sql.set("SHOW CREATE TABLE " + quote(tableId));
                        mysql.query(sql.get(), rs -> {
                            if (rs.next()) {
                                schema.applyDdl(source, dbName, rs.getString(2), this::enqueueSchemaChanges);
                            }
                        });
                    }
                }
                context.makeRecord().regenerate();

                // ------
                // STEP 7
                // ------
                if (minimalBlocking && isLocked) {
                    if (tableLocks) {
                        // We could not acquire a global read lock and instead had to obtain individual table-level read locks
                        // using 'FLUSH TABLE <tableName> WITH READ LOCK'. However, if we were to do this, the 'UNLOCK TABLES'
                        // would implicitly commit our active transaction, and this would break our consistent snapshot logic.
                        // Therefore, we cannot unlock the tables here!
                        // https://dev.mysql.com/doc/refman/5.7/en/flush.html
                        logger.info("Step {}: tables were locked explicitly, but to get a consistent snapshot we cannot "
                                + "release the locks until we've read all tables.", step++);
                    } else {
                        // We are doing minimal blocking via a global read lock, so we should release the global read lock now.
                        // All subsequent SELECT should still use the MVCC snapshot obtained when we started our transaction
                        // (since we started it "...with consistent snapshot"). So, since we're only doing very simple SELECT
                        // without WHERE predicates, we can release the lock now ...
                        logger.info("Step {}: releasing global read lock to enable MySQL writes", step);
                        sql.set("UNLOCK TABLES");
                        mysql.execute(sql.get());
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
                if (!isRunning()) return;
                if (includeData) {
                    BufferedBlockingConsumer<SourceRecord> bufferedRecordQueue = BufferedBlockingConsumer.bufferLast(super::enqueueRecord);

                    // Dump all of the tables and generate source records ...
                    logger.info("Step {}: scanning contents of {} tables while still in transaction", step, tableIds.size());
                    metrics.setTableCount(tableIds.size());

                    long startScan = clock.currentTimeInMillis();
                    AtomicLong totalRowCount = new AtomicLong();
                    int counter = 0;
                    int completedCounter = 0;
                    long largeTableCount = context.rowCountForLargeTable();
                    Iterator<TableId> tableIdIter = tableIds.iterator();
                    while (tableIdIter.hasNext()) {
                        TableId tableId = tableIdIter.next();
                        if (!isRunning()) break;

                        // Obtain a record maker for this table, which knows about the schema ...
                        RecordsForTable recordMaker = context.makeRecord().forTable(tableId, null, bufferedRecordQueue);
                        if (recordMaker != null) {

                            // Switch to the table's database ...
                            sql.set("USE " + quote(tableId.catalog()) + ";");
                            mysql.execute(sql.get());

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
                                        if (rs.next()) numRows.set(rs.getLong(5));
                                    });
                                    if (numRows.get() <= largeTableCount) {
                                        statementFactory = this::createStatement;
                                    }
                                    rowCountStr.set(numRows.toString());
                                } catch (SQLException e) {
                                    // Log it, but otherwise just use large result set by default ...
                                    logger.debug("Error while getting number of rows in table {}: {}", tableId, e.getMessage(), e);
                                }
                            }

                            // Scan the rows in the table ...
                            long start = clock.currentTimeInMillis();
                            logger.info("Step {}: - scanning table '{}' ({} of {} tables)", step, tableId, ++counter, tableIds.size());

                            Map<TableId, String> selectOverrides = getSnapshotSelectOverridesByTable();

                            String selectStatement = selectOverrides.getOrDefault(tableId, "SELECT * FROM " + quote(tableId));
                            logger.info("For table '{}' using select statement: '{}'", tableId, selectStatement);
                            sql.set(selectStatement);

                            try {
                                int stepNum = step;
                                mysql.query(sql.get(), statementFactory, rs -> {
                                    long rowNum = 0;
                                    try {
                                        // The table is included in the connector's filters, so process all of the table records
                                        // ...
                                        final Table table = schema.tableFor(tableId);
                                        final int numColumns = table.columns().size();
                                        final Object[] row = new Object[numColumns];
                                        while (rs.next()) {
                                            for (int i = 0, j = 1; i != numColumns; ++i, ++j) {
                                                Column actualColumn = table.columns().get(i);
                                                row[i] = readField(rs, j, actualColumn);
                                            }
                                            recorder.recordRow(recordMaker, row, ts); // has no row number!
                                            ++rowNum;
                                            if (rowNum % 100 == 0 && !isRunning()) {
                                                // We've stopped running ...
                                                break;
                                            }
                                            if (rowNum % 10_000 == 0) {
                                                long stop = clock.currentTimeInMillis();
                                                logger.info("Step {}: - {} of {} rows scanned from table '{}' after {}",
                                                            stepNum, rowNum, rowCountStr, tableId, Strings.duration(stop - start));
                                            }
                                        }

                                        totalRowCount.addAndGet(rowNum);
                                        if (isRunning()) {
                                            long stop = clock.currentTimeInMillis();
                                            logger.info("Step {}: - Completed scanning a total of {} rows from table '{}' after {}",
                                                        stepNum, rowNum, tableId, Strings.duration(stop - start));
                                        }
                                    } catch (InterruptedException e) {
                                        Thread.interrupted();
                                        // We were not able to finish all rows in all tables ...
                                        logger.info("Step {}: Stopping the snapshot due to thread interruption", stepNum);
                                        interrupted.set(true);
                                    }
                                });
                            } finally {
                                metrics.completeTable();
                                if (interrupted.get()) break;
                            }
                        }
                        ++completedCounter;
                    }

                    // See if we've been stopped or interrupted ...
                    if (!isRunning() || interrupted.get()) return;

                    // We've copied all of the tables and we've not yet been stopped, but our buffer holds onto the
                    // very last record. First mark the snapshot as complete and then apply the updated offset to
                    // the buffered record ...
                    source.markLastSnapshot();
                    long stop = clock.currentTimeInMillis();
                    try {
                        bufferedRecordQueue.close(this::replaceOffset);
                        logger.info("Step {}: scanned {} rows in {} tables in {}",
                                    step, totalRowCount, tableIds.size(), Strings.duration(stop - startScan));
                    } catch (InterruptedException e) {
                        Thread.interrupted();
                        // We were not able to finish all rows in all tables ...
                        logger.info("Step {}: aborting the snapshot after {} rows in {} of {} tables {}",
                                    step, totalRowCount, completedCounter, tableIds.size(), Strings.duration(stop - startScan));
                        interrupted.set(true);
                    }
                } else {
                    // source.markLastSnapshot(); Think we will not be needing this here it is used to mark last row entry?
                    logger.info("Step {}: encountered only schema based snapshot, skipping data snapshot", step);
                }
                step++;
            } finally {
                // No matter what, we always want to do these steps if necessary ...

                // ------
                // STEP 9
                // ------
                // Either commit or roll back the transaction, BEFORE releasing the locks ...
                if (isTxnStarted) {
                    if (interrupted.get() || !isRunning()) {
                        // We were interrupted or were stopped while reading the tables,
                        // so roll back the transaction and return immediately ...
                        logger.info("Step {}: rolling back transaction after abort", step++);
                        sql.set("ROLLBACK");
                        mysql.execute(sql.get());
                        metrics.abortSnapshot();
                        return;
                    }
                    // Otherwise, commit our transaction
                    logger.info("Step {}: committing transaction", step++);
                    sql.set("COMMIT");
                    mysql.execute(sql.get());
                    metrics.completeSnapshot();
                }

                // -------
                // STEP 10
                // -------
                // Release the read lock(s) if we have not yet done so. Locks are not released when committing/rolling back ...
                if (isLocked) {
                    if (tableLocks) {
                        logger.info("Step {}: releasing table read locks to enable MySQL writes", step++);
                    } else {
                        logger.info("Step {}: releasing global read lock to enable MySQL writes", step++);
                    }
                    sql.set("UNLOCK TABLES");
                    mysql.execute(sql.get());
                    isLocked = false;
                    long lockReleased = clock.currentTimeInMillis();
                    metrics.globalLockReleased();
                    if (tableLocks) {
                        logger.info("Writes to MySQL prevented for a total of {}", Strings.duration(lockReleased - lockAcquired));
                    } else {
                        logger.info("Writes to MySQL tables prevented for a total of {}", Strings.duration(lockReleased - lockAcquired));
                    }
                }
            }

            if (!isRunning()) {
                // The reader (and connector) was stopped and we did not finish ...
                try {
                    // Mark this reader as having completing its work ...
                    completeSuccessfully();
                    long stop = clock.currentTimeInMillis();
                    logger.info("Stopped snapshot after {} but before completing", Strings.duration(stop - ts));
                } finally {
                    // and since there's no more work to do clean up all resources ...
                    cleanupResources();
                }
            } else {
                // We completed the snapshot...
                try {
                    // Mark the source as having completed the snapshot. This will ensure the `source` field on records
                    // are not denoted as a snapshot ...
                    source.completeSnapshot();
                } finally {
                    // Set the completion flag ...
                    completeSuccessfully();
                    long stop = clock.currentTimeInMillis();
                    logger.info("Completed snapshot in {}", Strings.duration(stop - ts));
                }
            }
        } catch (Throwable e) {
            failed(e, "Aborting snapshot due to error when last running '" + sql.get() + "': " + e.getMessage());
        }
    }

    protected void readBinlogPosition(int step, SourceInfo source, JdbcConnection mysql, AtomicReference<String> sql) throws SQLException {
        logger.info("Step {}: read binlog position of MySQL master", step);
        String showMasterStmt = "SHOW MASTER STATUS";
        sql.set(showMasterStmt);
        mysql.query(sql.get(), rs -> {
            if (rs.next()) {
                String binlogFilename = rs.getString(1);
                long binlogPosition = rs.getLong(2);
                source.setBinlogStartPoint(binlogFilename, binlogPosition);
                if (rs.getMetaData().getColumnCount() > 4) {
                    // This column exists only in MySQL 5.6.5 or later ...
                    String gtidSet = rs.getString(5);// GTID set, may be null, blank, or contain a GTID set
                    source.setCompletedGtidSet(gtidSet);
                    logger.info("\t using binlog '{}' at position '{}' and gtid '{}'", binlogFilename, binlogPosition,
                                gtidSet);
                } else {
                    logger.info("\t using binlog '{}' at position '{}'", binlogFilename, binlogPosition);
                }
                source.startSnapshot();
            } else {
                throw new IllegalStateException("Cannot read the binlog filename and position via '" + showMasterStmt
                        + "'. Make sure your server is correctly configured");
            }
        });
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
        Statement stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE);
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
        } catch (SQLException e) {
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
            } else {
                logger.info("Snapshot is using user '{}' with these MySQL grants:", mysql.username());
                grants.forEach(grant -> logger.info("\t{}", grant));
            }
        } catch (SQLException e) {
            logger.info("Cannot determine the privileges for '{}' ", mysql.username(), e);
        }
    }

    /**
     * Utility method to replace the offset in the given record with the latest. This is used on the last record produced
     * during the snapshot.
     *
     * @param record the record
     * @return the updated record
     */
    protected SourceRecord replaceOffset(SourceRecord record) {
        if (record == null) return null;
        Map<String, ?> newOffset = context.source().offset();
        return new SourceRecord(record.sourcePartition(),
                newOffset,
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                record.value());
    }

    protected void enqueueSchemaChanges(String dbName, String ddlStatement) {
        if (!context.includeSchemaChangeRecords() || ddlStatement.length() == 0) {
            return;
        }
        if (context.makeRecord().schemaChanges(dbName, ddlStatement, super::enqueueRecord) > 0) {
            logger.info("\t{}", ddlStatement);
        }
    }

    protected void recordRowAsRead(RecordsForTable recordMaker, Object[] row, long ts) throws InterruptedException {
        recordMaker.read(row, ts);
    }

    protected void recordRowAsInsert(RecordsForTable recordMaker, Object[] row, long ts) throws InterruptedException {
        recordMaker.create(row, ts);
    }

    /**
     * Returns any SELECT overrides, if present.
     */
    private Map<TableId, String> getSnapshotSelectOverridesByTable() {
        String tableList = context.getSnapshotSelectOverrides();

        if (tableList == null) {
            return Collections.emptyMap();
        }

        Map<TableId, String> snapshotSelectOverridesByTable = new HashMap<>();

        for (String table : tableList.split(",")) {
            snapshotSelectOverridesByTable.put(
                TableId.parse(table),
                context.config.getString(MySqlConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE + "." + table)
            );
        }

        return snapshotSelectOverridesByTable;
    }

    protected static interface RecordRecorder {
        void recordRow(RecordsForTable recordMaker, Object[] row, long ts) throws InterruptedException;
    }
}
