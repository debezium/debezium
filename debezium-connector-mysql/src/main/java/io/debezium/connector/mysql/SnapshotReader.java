/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.debezium.connector.mysql.RecordMakers.RecordsForTable;
import io.debezium.function.Predicates;
import io.debezium.jdbc.JdbcConnection;
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
    private RecordRecorder recorder;
    private volatile Thread thread;
    private volatile Runnable onSuccessfulCompletion;

    /**
     * Create a snapshot reader.
     * 
     * @param context the task context in which this reader is running; may not be null
     */
    public SnapshotReader(MySqlTaskContext context) {
        super(context);
        recorder = this::recordRowAsRead;
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

    /**
     * Stop the snapshot from running.
     */
    @Override
    protected void doStop() {
        thread.interrupt();
    }

    @Override
    protected void doCleanup() {
        this.thread = null;
        logger.trace("Completed writing all snapshot records");

        try {
            // Call the completion function to say that we've successfully completed
            if (onSuccessfulCompletion != null) onSuccessfulCompletion.run();
        } catch (Throwable e) {
            logger.error("Error calling completion function after completing snapshot", e);
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
        logRolesForCurrentUser(sql, mysql);
        logServerInformation(sql, mysql);
        try {
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
            sql.set("SHOW MASTER STATUS");
            mysql.query(sql.get(), rs -> {
                if (rs.next()) {
                    String binlogFilename = rs.getString(1);
                    long binlogPosition = rs.getLong(2);
                    source.setBinlogStartPoint(binlogFilename, binlogPosition);
                    if (rs.getMetaData().getColumnCount() > 4) {
                        // This column exists only in MySQL 5.6.5 or later ...
                        String gtidSet = rs.getString(5);// GTID set, may be null, blank, or contain a GTID set
                        source.setGtidSet(gtidSet);
                        logger.debug("\t using binlog '{}' at position '{}' and gtid '{}'", binlogFilename, binlogPosition,
                                     gtidSet);
                    } else {
                        logger.debug("\t using binlog '{}' at position '{}'", binlogFilename, binlogPosition);
                    }
                    source.startSnapshot();
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
            logger.debug("\t list of available databases is: {}", databaseNames);
           
            // ------
            // STEP 5
            // ------
            // Get the list of table IDs for each database. We can't use a prepared statement with MySQL, so we have to
            // build the SQL statement each time. Although in other cases this might lead to SQL injection, in our case
            // we are reading the database names from the database and not taking them from the user ...
            logger.info("Step 5: read list of available tables in each database");
            final List<TableId> tableIds = new ArrayList<>();
            final Map<String, List<TableId>> tableIdsByDbName = new HashMap<>();
            for (String dbName : databaseNames) {
                sql.set("SHOW TABLES IN " + dbName);
                mysql.query(sql.get(), rs -> {
                    while (rs.next()) {
                        TableId id = new TableId(dbName, null, rs.getString(1));
                        if (filters.tableFilter().test(id)) {
                            tableIds.add(id);
                            tableIdsByDbName.computeIfAbsent(dbName, k -> new ArrayList<>()).add(id);
                            logger.debug("\t including '{}'", id);
                        } else {
                            logger.debug("\t '{}' is filtered out, discarding", id);
                        }
                    }
                });
            }

            // ------
            // STEP 6
            // ------
            // Transform the current schema so that it reflects the *current* state of the MySQL server's contents.
            // First, get the DROP TABLE and CREATE TABLE statement (with keys and constraint definitions) for our tables ...
            logger.info("Step 6: generating DROP and CREATE statements to reflect current database schemas");
            final List<String> ddlStatements = new ArrayList<>();
            // Add DROP TABLE statements for all tables that we knew about AND those tables found in the databases ...
            Set<TableId> allTableIds = new HashSet<>(schema.tables().tableIds());
            allTableIds.addAll(tableIds);
            allTableIds.forEach(tableId -> {
                ddlStatements.add("DROP TABLE IF EXISTS " + tableId);
            });
            // Add a DROP DATABASE statement for each database that we no longer know about ...
            schema.tables().tableIds().stream().map(TableId::catalog)
                  .filter(Predicates.not(databaseNames::contains))
                  .forEach(missingDbName -> {
                      ddlStatements.add("DROP DATABASE IF EXISTS " + missingDbName);
                  });
            // Now process all of our tables for each database ...
            for (Map.Entry<String, List<TableId>> entry : tableIdsByDbName.entrySet()) {
                String dbName = entry.getKey();
                // First drop, create, and then use the named database ...
                ddlStatements.add("DROP DATABASE IF EXISTS " + dbName);
                ddlStatements.add("CREATE DATABASE " + dbName);
                ddlStatements.add("USE " + dbName);
                for (TableId tableId : entry.getValue()) {
                    sql.set("SHOW CREATE TABLE " + tableId);
                    mysql.query(sql.get(), rs -> {
                        if (rs.next()) ddlStatements.add(rs.getString(2)); // CREATE TABLE statement
                    });
                }
            }
            // Finally, apply the DDL statements to the schema and then update the record maker...
            logger.debug("Step 6b: applying DROP and CREATE statements to connector's table model");
            String ddlStatementsStr = String.join(";" + System.lineSeparator(), ddlStatements);
            schema.applyDdl(source, null, ddlStatementsStr, this::enqueueSchemaChanges);
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
                logger.info("Writes to MySQL prevented for a total of {}", Strings.duration(lockReleased - lockAcquired));
            }

            // ------
            // STEP 8
            // ------
            // Dump all of the tables and generate source records ...
            logger.info("Step 8: scanning contents of {} tables", tableIds.size());
            long startScan = clock.currentTimeInMillis();
            AtomicBoolean interrupted = new AtomicBoolean(false);
            int counter = 0;
            Iterator<TableId> tableIdIter = tableIds.iterator();
            while (tableIdIter.hasNext()) {
                TableId tableId = tableIdIter.next();
                boolean isLastTable = tableIdIter.hasNext() == false;
                long start = clock.currentTimeInMillis();
                logger.debug("Step 8.{}: scanning table '{}'; {} tables remain", ++counter, tableId, tableIds.size() - counter);
                sql.set("SELECT * FROM " + tableId);
                mysql.query(sql.get(), rs -> {
                    RecordsForTable recordMaker = context.makeRecord().forTable(tableId, null, super::enqueueRecord);
                    if (recordMaker != null) {
                        boolean completed = false;
                        try {
                            // The table is included in the connector's filters, so process all of the table records ...
                            final Table table = schema.tableFor(tableId);
                            final int numColumns = table.columns().size();
                            final Object[] row = new Object[numColumns];
                            while (rs.next()) {
                                for (int i = 0, j = 1; i != numColumns; ++i, ++j) {
                                    row[i] = rs.getObject(j);
                                }
                                if (isLastTable && rs.isLast()) {
                                    // This is the last record, so mark the offset as having completed the snapshot
                                    // but the SourceInfo.struct() will still be marked as being part of the snapshot ...
                                    source.markLastSnapshot();
                                }
                                recorder.recordRow(recordMaker, row, ts); // has no row number!
                            }
                            if (isLastTable) completed = true;
                        } catch (InterruptedException e) {
                            Thread.interrupted();
                            if (!completed) {
                                // We were not able to finish all rows in all tables ...
                                logger.info("Stopping the snapshot after thread interruption");
                                interrupted.set(true);
                            }
                        }
                    }
                });
                if (interrupted.get()) break;
                long stop = clock.currentTimeInMillis();
                logger.info("Step 8.{}: scanned table '{}' in {}", counter, tableId, Strings.duration(stop - start));
            }
            long stop = clock.currentTimeInMillis();
            logger.info("Step 8: scanned contents of {} tables in {}", tableIds.size(), Strings.duration(stop - startScan));

            // ------
            // STEP 9
            // ------
            // Release the read lock if we have not yet done so ...
            if (!unlocked) {
                logger.info("Step 9: releasing global read lock to enable MySQL writes");
                sql.set("UNLOCK TABLES");
                mysql.execute(sql.get());
                unlocked = true;
                long lockReleased = clock.currentTimeInMillis();
                logger.info("Writes to MySQL prevented for a total of {}", Strings.duration(lockReleased - lockAcquired));
            }

            // -------
            // STEP 10
            // -------
            if (interrupted.get()) {
                // We were interrupted while reading the tables, so roll back the transaction and return immediately ...
                logger.info("Step 10: rolling back transaction after request to stop");
                sql.set("ROLLBACK");
                mysql.execute(sql.get());
                return;
            }
            // Otherwise, commit our transaction
            logger.info("Step 10: committing transaction");
            sql.set("COMMIT");
            mysql.execute(sql.get());

            try {
                // Mark the source as having completed the snapshot. This will ensure the `source` field on records
                // are not denoted as a snapshot ...
                source.completeSnapshot();
            } finally {
                // Set the completion flag ...
                super.completeSuccessfully();
                stop = clock.currentTimeInMillis();
                logger.info("Completed snapshot in {}", Strings.duration(stop - ts));
            }
        } catch (Throwable e) {
            failed(e, "Aborting snapshot after running '" + sql.get() + "': " + e.getMessage());
        }
    }
    
    private void logServerInformation(AtomicReference<String> sql, JdbcConnection mysql) {
        try {
            sql.set("SHOW VARIABLES LIKE 'version'");
            mysql.query(sql.get(), rs -> {
                if (rs.next()) {
                    logger.info("MySql server version is '{}'", rs.getString(2));        
                }
            });
        } catch (SQLException e) {
            logger.info("Cannot determine MySql server version", e);
        }       
    }

    private void logRolesForCurrentUser(AtomicReference<String> sql, JdbcConnection mysql) {
        try {
            List<String> privileges = new ArrayList<>();
            sql.set("SHOW GRANTS");
            mysql.query(sql.get(), rs -> {
                while (rs.next()) {
                    privileges.add(rs.getString(1));
                }
            });
            logger.info("User '{}' has '{}'", mysql.username(), privileges);
        } catch (SQLException e) {
            logger.info("Cannot determine the privileges for '{}' ", mysql.username(), e);
        }
    }

    protected void enqueueSchemaChanges(String dbName, String ddlStatements) {
        if (context.includeSchemaChangeRecords() &&
                context.makeRecord().schemaChanges(dbName, ddlStatements, super::enqueueRecord) > 0) {
            logger.debug("Recorded DDL statements for database '{}': {}", dbName, ddlStatements);
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
