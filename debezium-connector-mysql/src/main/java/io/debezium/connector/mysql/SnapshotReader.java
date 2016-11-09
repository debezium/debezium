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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.mysql.RecordMakers.RecordsForTable;
import io.debezium.function.BufferedBlockingConsumer;
import io.debezium.function.Predicates;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcConnection.StatementFactory;
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

    private boolean minimalBlocking = true;
    private final boolean includeData;
    private RecordRecorder recorder;
    private volatile Thread thread;
    private volatile Runnable onSuccessfulCompletion;
    private final SnapshotReaderMetrics metrics;

    /**
     * Create a snapshot reader.
     * 
     * @param context the task context in which this reader is running; may not be null
     */
    public SnapshotReader(MySqlTaskContext context) {
        super(context);
        this.includeData = !context.isSchemaOnlySnapshot();
        recorder = this::recordRowAsRead;
        metrics = new SnapshotReaderMetrics(context.clock());
        metrics.register(context, logger);
    }

    /**
     * Set the non-blocking function that should be called upon successful completion of the snapshot, which is after the
     * snapshot generates its final record <em>and</em> all such records have been {@link #poll() polled}.
     * 
     * @param onSuccessfulCompletion the function; may be null
     * @return this object for method chaining; never null
     */
    public SnapshotReader onSuccessfulCompletion(Runnable onSuccessfulCompletion) {
        this.onSuccessfulCompletion = onSuccessfulCompletion;
        return this;
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

    /**
     * Start the snapshot and return immediately. Once started, the records read from the database can be retrieved using
     * {@link #poll()} until that method returns {@code null}.
     */
    @Override
    protected void doStart() {
        thread = new Thread(this::execute, "mysql-snapshot-" + context.serverName());
        // TODO: Use MDC logging
        thread.start();
    }

    @Override
    protected void doStop() {
        thread.interrupt();
    }

    @Override
    protected void doShutdown() {
        metrics.unregister(logger);
    }

    @Override
    protected void doCleanup() {
        this.thread = null;
        logger.trace("Completed writing all snapshot records");

        try {
            // Call the completion function to say that we've successfully completed
            if (onSuccessfulCompletion != null) onSuccessfulCompletion.run();
        } catch (RuntimeException e) {
            throw e;
        } catch (Throwable e) {
            throw new ConnectException("Error calling completion function after completing snapshot", e);
        }
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
            logger.info("Step 0: disabling autocommit and enabling repeatable read transactions");
            mysql.setAutoCommit(false);
            sql.set("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            mysql.execute(sql.get());
            metrics.globalLockAcquired();

            // Generate the DDL statements that set the charset-related system variables ...
            Map<String, String> systemVariables = context.readMySqlCharsetSystemVariables(sql);
            String setSystemVariablesStatement = context.setStatementFor(systemVariables);

            // ------
            // STEP 1
            // ------
            // First, start a transaction and request that a consistent MVCC snapshot is obtained immediately.
            // See http://dev.mysql.com/doc/refman/5.7/en/commit.html
            logger.info("Step 1: start transaction with consistent snapshot");
            sql.set("START TRANSACTION WITH CONSISTENT SNAPSHOT");
            mysql.execute(sql.get());

            // ------
            // STEP 2
            // ------
            // Obtain read lock on all tables. This statement closes all open tables and locks all tables
            // for all databases with a global read lock, and it prevents ALL updates while we have this lock.
            // It also ensures that everything we do while we have this lock will be consistent.
            long lockAcquired = clock.currentTimeInMillis();
            logger.info("Step 2: flush and obtain global read lock (preventing writes to database)");
            sql.set("FLUSH TABLES WITH READ LOCK");
            mysql.execute(sql.get());

            // ------
            // STEP 3
            // ------
            // Obtain the binlog position and update the SourceInfo in the context. This means that all source records generated
            // as part of the snapshot will contain the binlog position of the snapshot.
            logger.info("Step 3: read binlog position of MySQL master");
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

            // From this point forward, all source records produced by this connector will have an offset that includes a
            // "snapshot" field (with value of "true").

            // ------
            // STEP 4
            // ------
            // Get the list of databases ...
            logger.info("Step 4: read list of available databases");
            final List<String> databaseNames = new ArrayList<>();
            sql.set("SHOW DATABASES");
            mysql.query(sql.get(), rs -> {
                while (rs.next()) {
                    databaseNames.add(rs.getString(1));
                }
            });
            logger.info("\t list of available databases is: {}", databaseNames);

            // ------
            // STEP 5
            // ------
            // Get the list of table IDs for each database. We can't use a prepared statement with MySQL, so we have to
            // build the SQL statement each time. Although in other cases this might lead to SQL injection, in our case
            // we are reading the database names from the database and not taking them from the user ...
            logger.info("Step 5: read list of available tables in each database");
            List<TableId> tableIds = new ArrayList<>();
            final Map<String, List<TableId>> tableIdsByDbName = new HashMap<>();
            for (String dbName : databaseNames) {
                sql.set("SHOW TABLES IN " + dbName);
                mysql.query(sql.get(), rs -> {
                    while (rs.next()) {
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
            }

            // ------
            // STEP 6
            // ------
            // Transform the current schema so that it reflects the *current* state of the MySQL server's contents.
            // First, get the DROP TABLE and CREATE TABLE statement (with keys and constraint definitions) for our tables ...
            logger.info("Step 6: generating DROP and CREATE statements to reflect current database schemas:");
            schema.applyDdl(source, null, setSystemVariablesStatement, this::enqueueSchemaChanges);

            // Add DROP TABLE statements for all tables that we knew about AND those tables found in the databases ...
            Set<TableId> allTableIds = new HashSet<>(schema.tables().tableIds());
            allTableIds.addAll(tableIds);
            allTableIds.forEach(tableId -> schema.applyDdl(source, tableId.schema(), "DROP TABLE IF EXISTS " + tableId,
                                                           this::enqueueSchemaChanges));
            // Add a DROP DATABASE statement for each database that we no longer know about ...
            schema.tables().tableIds().stream().map(TableId::catalog)
                  .filter(Predicates.not(databaseNames::contains))
                  .forEach(missingDbName -> schema.applyDdl(source, missingDbName, "DROP DATABASE IF EXISTS " + missingDbName,
                                                            this::enqueueSchemaChanges));
            // Now process all of our tables for each database ...
            for (Map.Entry<String, List<TableId>> entry : tableIdsByDbName.entrySet()) {
                String dbName = entry.getKey();
                // First drop, create, and then use the named database ...
                schema.applyDdl(source, dbName, "DROP DATABASE IF EXISTS " + dbName, this::enqueueSchemaChanges);
                schema.applyDdl(source, dbName, "CREATE DATABASE " + dbName, this::enqueueSchemaChanges);
                schema.applyDdl(source, dbName, "USE " + dbName, this::enqueueSchemaChanges);
                for (TableId tableId : entry.getValue()) {
                    sql.set("SHOW CREATE TABLE " + tableId);
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
            boolean unlocked = false;
            if (minimalBlocking) {
                // We are doing minimal blocking, then we should release the read lock now. All subsequent SELECT
                // should still use the MVCC snapshot obtained when we started our transaction (since we started it
                // "...with consistent snapshot"). So, since we're only doing very simple SELECT without WHERE predicates,
                // we can release the lock now ...
                logger.info("Step 7: releasing global read lock to enable MySQL writes");
                sql.set("UNLOCK TABLES");
                mysql.execute(sql.get());
                unlocked = true;
                long lockReleased = clock.currentTimeInMillis();
                metrics.globalLockReleased();
                logger.info("Step 7: blocked writes to MySQL for a total of {}", Strings.duration(lockReleased - lockAcquired));
            }

            AtomicBoolean interrupted = new AtomicBoolean(false);
            // ------
            // STEP 8
            // ------
            // Use a buffered blocking consumer to buffer all of the records, so that after we copy all of the tables
            // and produce events we can update the very last event with the non-snapshot offset ...
            if (includeData) {
                BufferedBlockingConsumer<SourceRecord> bufferedRecordQueue = BufferedBlockingConsumer.bufferLast(super::enqueueRecord);

                // Dump all of the tables and generate source records ...
                logger.info("Step 8: scanning contents of {} tables", tableIds.size());
                metrics.setTableCount(tableIds.size());

                long startScan = clock.currentTimeInMillis();
                AtomicLong totalRowCount = new AtomicLong();
                int counter = 0;
                int completedCounter = 0;
                long largeTableCount = context.rowCountForLargeTable();
                Iterator<TableId> tableIdIter = tableIds.iterator();
                while (tableIdIter.hasNext()) {
                    TableId tableId = tableIdIter.next();

                    // Obtain a record maker for this table, which knows about the schema ...
                    RecordsForTable recordMaker = context.makeRecord().forTable(tableId, null, bufferedRecordQueue);
                    if (recordMaker != null) {

                        // Choose how we create statements based on the # of rows ...
                        sql.set("SELECT COUNT(*) FROM " + tableId);
                        AtomicLong numRows = new AtomicLong();
                        mysql.query(sql.get(), rs -> {
                            if (rs.next()) numRows.set(rs.getLong(1));
                        });
                        StatementFactory statementFactory = this::createStatement;
                        if (numRows.get() > largeTableCount) {
                            statementFactory = this::createStatementWithLargeResultSet;
                        }

                        // Scan the rows in the table ...
                        long start = clock.currentTimeInMillis();
                        logger.info("Step 8: - scanning table '{}' ({} of {} tables)", tableId, ++counter, tableIds.size());
                        sql.set("SELECT * FROM " + tableId);
                        mysql.query(sql.get(), statementFactory, rs -> {
                            long rowNum = 0;
                            long rowCount = numRows.get();
                            try {
                                // The table is included in the connector's filters, so process all of the table records ...
                                final Table table = schema.tableFor(tableId);
                                final int numColumns = table.columns().size();
                                final Object[] row = new Object[numColumns];
                                while (rs.next()) {
                                    for (int i = 0, j = 1; i != numColumns; ++i, ++j) {
                                        row[i] = rs.getObject(j);
                                    }
                                    recorder.recordRow(recordMaker, row, ts); // has no row number!
                                    ++rowNum;
                                    if (rowNum % 10_000 == 0 || rowNum == rowCount) {
                                        long stop = clock.currentTimeInMillis();
                                        logger.info("Step 8: - {} of {} rows scanned from table '{}' after {}", rowNum, rowCount, tableId,
                                                Strings.duration(stop - start));
                                    }
                                }
                            } catch (InterruptedException e) {
                                Thread.interrupted();
                                // We were not able to finish all rows in all tables ...
                                logger.info("Step 8: Stopping the snapshot due to thread interruption");
                                interrupted.set(true);
                            } finally {
                                totalRowCount.addAndGet(rowCount);
                            }
                        });

                        metrics.completeTable();
                        if (interrupted.get()) break;
                    }
                    ++completedCounter;
                }

                // We've copied all of the tables, but our buffer holds onto the very last record.
                // First mark the snapshot as complete and then apply the updated offset to the buffered record ...
                source.markLastSnapshot();
                long stop = clock.currentTimeInMillis();
                try {
                    bufferedRecordQueue.flush(this::replaceOffset);
                    logger.info("Step 8: scanned {} rows in {} tables in {}",
                            totalRowCount, tableIds.size(), Strings.duration(stop - startScan));
                } catch (InterruptedException e) {
                    Thread.interrupted();
                    // We were not able to finish all rows in all tables ...
                    logger.info("Step 8: aborting the snapshot after {} rows in {} of {} tables {}",
                            totalRowCount, completedCounter, tableIds.size(), Strings.duration(stop - startScan));
                    interrupted.set(true);
                }
            } else {
                // source.markLastSnapshot(); Think we will not be needing this here it is used to mark last row entry?
                logger.info("Step 8: encountered only schema based snapshot, skipping data snapshot");
            }

            // ------
            // STEP 9
            // ------
            // Release the read lock if we have not yet done so ...
            int step = 9;
            if (!unlocked) {
                logger.info("Step {}: releasing global read lock to enable MySQL writes", step++);
                sql.set("UNLOCK TABLES");
                mysql.execute(sql.get());
                unlocked = true;
                long lockReleased = clock.currentTimeInMillis();
                metrics.globalLockReleased();
                logger.info("Writes to MySQL prevented for a total of {}", Strings.duration(lockReleased - lockAcquired));
            }

            // -------
            // STEP 10
            // -------
            if (interrupted.get()) {
                // We were interrupted while reading the tables, so roll back the transaction and return immediately ...
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

            try {
                // Mark the source as having completed the snapshot. This will ensure the `source` field on records
                // are not denoted as a snapshot ...
                source.completeSnapshot();
            } finally {
                // Set the completion flag ...
                super.completeSuccessfully();
                long stop = clock.currentTimeInMillis();
                logger.info("Completed snapshot in {}", Strings.duration(stop - ts));
            }
        } catch (Throwable e) {
            failed(e, "Aborting snapshot after running '" + sql.get() + "': " + e.getMessage());
        }
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

    protected static interface RecordRecorder {
        void recordRow(RecordsForTable recordMaker, Object[] row, long ts) throws InterruptedException;
    }
}
