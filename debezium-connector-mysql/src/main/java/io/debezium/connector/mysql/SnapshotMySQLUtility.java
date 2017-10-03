package io.debezium.connector.mysql;

import io.debezium.connector.mysql.AbstractSnapshotReader.RecordRecorder;
import io.debezium.function.BufferedBlockingConsumer;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.ddl.DdlChanges.DatabaseStatementStringConsumer;
import io.debezium.util.Strings;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * A utility class for running MySQL snapshots.
 */
public class SnapshotMySQLUtility {

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private final SnapshotReaderMetrics metrics;

    private final MySqlTaskContext context;
    private final JdbcConnection mysql;
    // vvv I still have no idea what the point of this is?
    private final AtomicReference<String> sql = new AtomicReference<>();

    private boolean minimalBlocking = true;

    private List<TableId> tableIds = null;
    private Map<String, List<TableId>> tableIdsByDbName = null;
    private Set<String> readableDatabaseNames = null;

    private String binlogFilename;
    private long binlogPosition;
    private String gtidSet;

    private boolean activeTransaction = false;
    private boolean tablesLocked = false;
    private long lockAcquiredTsMs;
    private Collection<TableId> lockedTables = null;
    private AtomicBoolean interrupted = new AtomicBoolean(false); // I still don't totally understand what this is for.


    public SnapshotMySQLUtility(MySqlTaskContext context) {
        this.context = context;
        this.metrics = new SnapshotReaderMetrics(context.clock());
        this.mysql = context.jdbc();
    }

    /**
     * Set whether to block other transactions as minimally as possible by releasing the read lock
     * as early as possible (when buffer is first called). Although the snapshot process should
     * obtain a consistent snapshot even when releasing the lock as early as possible, it may be
     * desirable to explicitly hold onto the read lock until execution completes. In such cases,
     * holding onto the lock will prevent all updates to the database during the snapshot process.
     *
     * @param minimalBlocking {@code true} if the lock is to be released as early as possible, or
     *                        {@code false} if the lock is to be held for the entire transaction.
     * @return this object for method chaining; never null
     */
    public void useMinimalBlocking(boolean minimalBlocking) {
        this.minimalBlocking = minimalBlocking;
    }

    /**
     * Begin a transaction and read the binlog position. Required before calling
     * {@link #bufferLockedTables(RecordRecorder, BufferedBlockingConsumer)} or
     * {@link #bufferLockedTables(List, RecordRecorder, BufferedBlockingConsumer)}.
     * @throws SQLException
     */
    public void beginTransaction() throws SQLException {
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
        logger.info("\tdisabling autocommit and enabling repeatable read transactions");
        mysql.setAutoCommit(false);
        sql.set("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
        mysql.execute(sql.get());
        // Start a transaction
        // See http://dev.mysql.com/doc/refman/5.7/en/commit.html
        sql.set("START TRANSACTION WITH CONSISTENT SNAPSHOT");
        // todo I need to get the gtid of this transaction somehow.
        mysql.execute(sql.get());
        activeTransaction = true;

        readBinlogPosition();
    }

    /**
     * Read and internally store all avaliable databases, and all the tables that match the config.
     *
     * @throws SQLException
     * @see {@link #getTableIds()}, {@link #getTableIdsByDbName()}, {@link #getReadableDatabaseNames()}
     */
    public void readTableList() throws SQLException {
        // do we need to be in a transaction to do this?
        verifyActiveTransaction("reading databases and tables");

        // Get the list of databases ...
        logger.info("reading list of available databases");
        final List<String> databaseNames = new ArrayList<>();
        sql.set("SHOW DATABASES");
        mysql.query(sql.get(), rs -> {
            while (rs.next()) {
                databaseNames.add(rs.getString(1));
            }
        });
        logger.info("\t list of available databases is: {}", databaseNames);

        // Get the list of table IDs for each database. We can't use a prepared statement with MySQL, so we have to
        // build the SQL statement each time. Although in other cases this might lead to SQL injection, in our case
        // we are reading the database names from the database and not taking them from the user ...
        logger.info("reading list of available tables in each database");

        tableIds = new ArrayList<>();
        tableIdsByDbName = new HashMap<>();
        readableDatabaseNames = new HashSet<>();

        Filters filters = context.dbSchema().filters();
        for (String dbName : databaseNames) {
            try {
                // MySQL sometimes considers some local files as databases (see DBZ-164),
                // so we will simply try each one and ignore the problematic ones ...
                sql.set("SHOW FULL TABLES IN " + quote(dbName) + " where Table_Type = 'BASE TABLE'");
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
                readableDatabaseNames.add(dbName);
            } catch (SQLException e) {
                // We were unable to execute the query or process the results, so skip this ...
                logger.warn("\t skipping database '{}' due to error reading tables: {}", dbName, e.getMessage());
            }
        }
        final Set<String> includedDatabaseNames =
            readableDatabaseNames.stream().filter(filters.databaseFilter()).collect(Collectors.toSet());
        logger.info("\tsnapshot continuing with database(s): {}", includedDatabaseNames);

    }

    public boolean hasReadTables(){
        return tableIds != null;
    }

    /**
     * @return the list of tableIds that have been read, or null if no tables have been read.
     */
    public List<TableId> getTableIds() {
        return tableIds;
    }

    /**
     * @return a map from databases to tableIds of all the tables that have been read,
     *         or null if no tables have been read.
     */
    public Map<String, List<TableId>> getTableIdsByDbName() {
        return tableIdsByDbName;
    }

    /**
     * @return the list of database names that have been read, or null if no tables have been read.
     */
    public Set<String> getReadableDatabaseNames() {
        return readableDatabaseNames;
    }

    private void verifyActiveTransaction(String action) {
        if (!activeTransaction) {
            throw new IllegalStateException("Need to begin a transaction before " + action + "!");
        }
    }

    /**
     * Lock all the tables.
     * If possible, this will be done by acquiring the global lock.
     * If not possible, all tables will be individually locked.
     * Individual locking will result in reading the table list, if that has not been done.
     *
     * @throws SQLException
     */
    public void lockAllTables() throws SQLException {
        try {
            lockGlobal();
        } catch (SQLException e) {
            if (tableIds == null) {
                // todo should I always read the table list?
                readTableList();
            }
            lockTables(tableIds);
        }
    }

    /**
     * Acquire the global lock
     * @throws SQLException
     */
    public void lockGlobal() throws SQLException {
        verifyActiveTransaction("locking tables");
        sql.set("FLUSH TABLES WITH READ LOCK");
        mysql.execute(sql.get());

        lockAcquiredTsMs = context.clock().currentTimeInMillis();
        metrics.globalLockAcquired();
        tablesLocked = true;
    }

    /**
     * Lock a specific list of tables.
     *
     * @param tableIds the tables to lock.
     * @throws SQLException
     */
    public void lockTables(Collection<TableId> tableIds) throws SQLException {
        verifyActiveTransaction("locking tables");

        if (!context.userHasPrivileges("LOCK TABLES")) {
            // We don't have the right privileges
            throw new ConnectException("User does not have the 'LOCK TABLES' privilege required to obtain a "
                + "consistent snapshot by preventing concurrent writes to tables.");
        }
        // We have the required privileges, so try to lock all of the tables we're interested in ...
        String tableList = tableIds.stream()
            .map(tid -> quote(tid))
            .reduce((r, element) -> r+ "," + element)
            .orElse(null);
        if (tableList != null) {
            sql.set("FLUSH TABLES " + tableList + " WITH READ LOCK");
            mysql.execute(sql.get());
        }

        lockAcquiredTsMs = context.clock().currentTimeInMillis();
        metrics.globalLockAcquired();
        tablesLocked = true;
        lockedTables = new ArrayList<>(tableIds);
    }

    private void readBinlogPosition() throws SQLException {
        SourceInfo source = context.source();
        String showMasterStmt = "SHOW MASTER STATUS";
        sql.set(showMasterStmt);
        mysql.query(sql.get(), rs -> {
            if (rs.next()) {
                this.binlogFilename = rs.getString(1);
                this.binlogPosition = rs.getLong(2);
                //source.setBinlogStartPoint(binlogFilename, binlogPosition); // todo maybe this isn't a reasonable thing to do every time; this only makes sense for the INITIAL snapshot, not for in-line.
                if (rs.getMetaData().getColumnCount() > 4) {
                    // This column exists only in MySQL 5.6.5 or later ...
                    this.gtidSet = rs.getString(5);// GTID set, may be null, blank, or contain a GTID set
                    //source.setCompletedGtidSet(gtidSet); // todo not sure this is reasonable either...
                    logger.info("\t using binlog '{}' at position '{}' and gtid '{}'", binlogFilename, binlogPosition,
                        gtidSet);
                } else {
                    logger.info("\t using binlog '{}' at position '{}'", binlogFilename, binlogPosition);
                }
                source.startSnapshot();
                // still not a huge fan of this being in here.
            } else {
                throw new IllegalStateException("Cannot read the binlog filename and position via '" + showMasterStmt
                    + "'. Make sure your server is correctly configured");
            }
        });
    }

    public String getBinlogFilename() {
        return binlogFilename;
    }

    public long getBinlogPosition() {
        return binlogPosition;
    }

    public String getGtidSet() {
        return gtidSet;
    }

    public void bufferLockedTables(RecordRecorder recorder,
                                   BufferedBlockingConsumer<SourceRecord> bufferedRecordQueue) throws SQLException {
        bufferLockedTables(tableIds, recorder, bufferedRecordQueue);
    }

    public void createDropDatabaseEvents(Collection<String> databases,
                                         DatabaseStatementStringConsumer statementConsumer) throws SQLException {
        MySqlSchema schema = context.dbSchema();
        SourceInfo source = context.source();
        for(String database : databases) {
            schema.applyDdl(source,
                database,
                "DROP DATABASE IF EXISTS " + quote(database),
                statementConsumer);
        }
    }

    public void createDropTableEvents(Collection<TableId> tables,
                                      DatabaseStatementStringConsumer statementConsumer) throws SQLException {
        MySqlSchema schema = context.dbSchema();
        SourceInfo source = context.source();
        for (TableId tableId : tables) {
            schema.applyDdl(source,
                tableId.schema(),
                "DROP TABLE IF EXISTS " + quote(tableId),
                statementConsumer);
        }
    }

    public void createCreateDatabaseEvents(Collection<String> databases,
                                           DatabaseStatementStringConsumer statementConsumer) throws SQLException {
        MySqlSchema schema = context.dbSchema();
        SourceInfo source = context.source();
        for (String database: databases) {
            schema.applyDdl(source, database, "CREATE DATABASE " + quote(database), statementConsumer);
        }
    }

    /**
     * Create 'CREATE' events for the given tables.
     * @param tables The collection of tables to create 'CREATE' events for.
     */
    public void createCreateTableEvents(Collection<TableId> tables,
                                        DatabaseStatementStringConsumer statementConsumer) throws SQLException {
        // todo I could make this a bit less repetitive by grouping the tables by database.
        MySqlSchema schema = context.dbSchema();
        SourceInfo source = context.source();
        for (TableId tableId : tables) {
            String database = tableId.catalog();
            // put us in the correct database
            // todo is this really needed?
            schema.applyDdl(source, database, "USE " + quote(database), statementConsumer);
            // create the table.
            sql.set("SHOW CREATE TABLE " + quote(tableId));
            mysql.query(sql.get(), rs -> {
                if (rs.next()) {
                    schema.applyDdl(source,
                        database,
                        rs.getString(2),
                        statementConsumer);
                }
            });
        }
    }

    /**
     *
     * @param recorder
     * @param bufferedRecordQueue
     * @throws SQLException
     */
    // todo method input order?
    // todo buffer a given list of tables (those tables must be locked, of course)
    // but that way we can lock all the tables but only buffer/read some tables.  Which is useful because then we
    // can release the lock earlier than we could normally.
    public void bufferLockedTables(List<TableId> tableIdsToBuffer,
                                   RecordRecorder recorder,
                                   BufferedBlockingConsumer<SourceRecord> bufferedRecordQueue)
        throws SQLException {
        if (!tablesLocked) {
            throw new IllegalStateException("No locked tables to buffer");
        }

        // todo: verify that all tables to be buffered are locked?

        final long bufferingStartTs = context.clock().currentTimeInMillis();
        final MySqlSchema schema = context.dbSchema();

        if (minimalBlocking) {
            if (lockedTables != null) {
                // We could not acquire a global read lock and instead had to obtain individual table-level read locks
                // using 'FLUSH TABLE <tableName> WITH READ LOCK'. However, if we were to do this, the 'UNLOCK TABLES'
                // would implicitly commit our active transaction, and this would break our consistent snapshot logic.
                // Therefore, we cannot unlock the tables here!
                // https://dev.mysql.com/doc/refman/5.7/en/flush.html
                logger.info("tables were locked explicitly, but to get a consistent snapshot we cannot "
                    + "release the locks until we've read all tables.");
            } else {
                // We are doing minimal blocking via a global read lock, so we should release the global read lock now.
                // All subsequent SELECT should still use the MVCC snapshot obtained when we started our transaction
                // (since we started it "...with consistent snapshot"). So, since we're only doing very simple SELECT
                // without WHERE predicates, we can release the lock now ...
                logger.info("releasing global read lock to enable MySQL writes");
                unlockTables();
            }
        }

        // Dump all of the tables and generate source records ...
        logger.info("scanning contents of {} tables while still in transaction", tableIdsToBuffer.size());
        metrics.setTableCount(tableIdsToBuffer.size());

        long startScan = context.clock().currentTimeInMillis();
        AtomicLong totalRowCount = new AtomicLong();
        int counter = 0;
        int completedCounter = 0;
        long largeTableCount = context.rowCountForLargeTable();
        Iterator<TableId> tableIdIter = tableIdsToBuffer.iterator();
        while (tableIdIter.hasNext()) {
            TableId tableId = tableIdIter.next();

            // Obtain a record maker for this table, which knows about the schema ...
            RecordMakers.RecordsForTable recordMaker = context.makeRecord().forTable(tableId, null, bufferedRecordQueue);
            if (recordMaker != null) {

                // Switch to the table's database ...
                sql.set("USE " + quote(tableId.catalog()) + ";");
                mysql.execute(sql.get());

                AtomicLong numRows = new AtomicLong(-1);
                AtomicReference<String> rowCountStr = new AtomicReference<>("<unknown>");
                JdbcConnection.StatementFactory statementFactory = this::createStatementWithLargeResultSet;
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
                long start = context.clock().currentTimeInMillis();
                logger.info("scanning table '{}' ({} of {} tables)", tableId, ++counter, tableIds.size());
                sql.set("SELECT * FROM " + quote(tableId));
                try {
                    mysql.query(sql.get(), statementFactory, rs -> {
                        long rowNum = 0;
                        try {
                            // The table is included in the connector's filters, so process all of the table records
                            final Table table = schema.tableFor(tableId);
                            final int numColumns = table.columns().size();
                            final Object[] row = new Object[numColumns];
                            while (rs.next()) {
                                for (int i = 0, j = 1; i != numColumns; ++i, ++j) {
                                    row[i] = rs.getObject(j);
                                }
                                recorder.recordRow(recordMaker, row, bufferingStartTs); // has no row number!
                                ++rowNum;
                                if (rowNum % 10_000 == 0) {
                                    long stop = context.clock().currentTimeInMillis();
                                    logger.info("{} of {} rows scanned from table '{}' after {}",
                                        rowNum, rowCountStr, tableId, Strings.duration(stop - start));
                                }
                            }

                            totalRowCount.addAndGet(rowNum);
                        } catch (InterruptedException e) {
                            Thread.interrupted();
                            // We were not able to finish all rows in all tables ...
                            logger.info("Stopping the snapshot due to thread interruption");
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

        // We've copied all of the tables and we've not yet been stopped, but our buffer holds onto the
        // very last record. First mark the snapshot as complete and then apply the updated offset to
        // the buffered record ...
        context.source().markLastSnapshot();
        long stop = context.clock().currentTimeInMillis();
        try {
            bufferedRecordQueue.flush(this::replaceOffset);
            logger.info("scanned {} rows in {} tables in {}",
                totalRowCount, tableIdsToBuffer.size(), Strings.duration(stop - startScan));
        } catch (InterruptedException e) {
            Thread.interrupted();
            // We were not able to finish all rows in all tables ...
            logger.info("aborting the snapshot after {} rows in {} of {} tables {}",
                totalRowCount, completedCounter, tableIdsToBuffer.size(), Strings.duration(stop - startScan));
            interrupted.set(true);
        }
    }

    /**
     * Complete the current transaction, and unlock any tables that are still locked.
     */
    public void completeTransactionAndUnlockTables() throws SQLException {
        if (interrupted.get()) {
            // We were interrupted or were stopped while reading the tables,
            // so roll back the transaction and return immediately ...
            logger.info("rolling back transaction after abort");
            sql.set("ROLLBACK");
            mysql.execute(sql.get());
            metrics.abortSnapshot();
            unlockTables();
            return;
        }
        // Otherwise, commit our transaction
        logger.info("committing transaction");
        sql.set("COMMIT");
        mysql.execute(sql.get());
        activeTransaction = false;
        metrics.completeSnapshot();
        unlockTables();
        context.source().completeSnapshot();
    }

    /**
     * Unlock any locked tables.
     */
    public void unlockTables() throws SQLException {
        if (!tablesLocked) {
            // nothing to be done.
            return;
        }

        sql.set("UNLOCK TABLES");
        mysql.execute(sql.get());
        tablesLocked = false;
        lockedTables = null;
        long lockReleased = context.clock().currentTimeInMillis(); // I don't know what this is for either.
        metrics.globalLockReleased();
        logger.info("blocked writes to MySQL for a total of {}",
            Strings.duration(lockReleased - lockAcquiredTsMs));
    }

    /**
     * Utility method to replace the offset in the given record with the latest. This is used on the last record produced
     * during the snapshot.
     *
     * @param record the record
     * @return the updated record
     */
    private SourceRecord replaceOffset(SourceRecord record) {
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

    private String quote(String dbOrTableName) {
        return "`" + dbOrTableName + "`";
    }

    private String quote(TableId id) {
        return quote(id.catalog()) + "." + quote(id.table());
    }
}