package io.debezium.connector.mysql;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.errors.ConnectException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Abstract Snapshot Reader
 *
 * This should still exist
 */
public abstract class AbstractSnapshotReader extends AbstractReader {

    private boolean minimalBlocking = true;
    private final SnapshotReaderMetrics metrics;

    /**
     * Create a snapshot reader.
     *
     * @param name    the name of the reader
     * @param context the task context in which this reader is running; may not be null
     */
    public AbstractSnapshotReader(String name, MySqlTaskContext context) {
        super(name, context);
        metrics = new SnapshotReaderMetrics(context.clock());
        metrics.register(context, logger);
    }

    /**
     * Set whether this reader's {@link #doStart()}  execution} should block other transactions as minimally as possible by
     * releasing the read lock as early as possible. Although the snapshot process should obtain a consistent snapshot even
     * when releasing the lock as early as possible, it may be desirable to explicitly hold onto the read lock until execution
     * completes. In such cases, holding onto the lock will prevent all updates to the database during the snapshot process.
     *
     * @param minimalBlocking {@code true} if the lock is to be released as early as possible, or {@code false} if the lock
     *            is to be held for the entire {@link #doStart()} execution}
     * @return this object for method chaining; never null
     */
    public AbstractSnapshotReader useMinimalBlocking(boolean minimalBlocking) {
        this.minimalBlocking = minimalBlocking;
        return this;
    }

    /**
     * Set the transaction isolation level to REPEATABLE READ. This is the default, but the default can be changed
     * which is why we explicitly set it here.
     *
     * With REPEATABLE READ, all SELECT queries within the scope of a transaction will read
     * from the same MVCC snapshot. Thus each plain (non-locking) SELECT statements within the same transaction are
     * consistent also with respect to each other.
     *
     * @param mysql
     * @param sql
     * @throws SQLException
     * @see <a href:"https://dev.mysql.com/doc/refman/5.7/en/set-transaction.html">SET TRANSACTION syntax documentation</a>
     *      <a href:"https://dev.mysql.com/doc/refman/5.7/en/innodb-transaction-isolation-levels.html">Transaction isolation levels documentation</a>
     *      <a href:"https://dev.mysql.com/doc/refman/5.7/en/innodb-consistent-read.html">Consistent Nonlocking Reads documentation</a>
     */
    private void setTransactionIsolationLevelRepeatableRead(JdbcConnection mysql, AtomicReference<String> sql)
            throws SQLException {
        logger.info("\tdisabling autocommit and enabling repeatable read transactions");
        mysql.setAutoCommit(false);
        sql.set("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
        mysql.execute(sql.get());
    }

    /**
     *
     */
    protected void startTransaction(JdbcConnection mysql, AtomicReference<String> sql) throws SQLException {
        // set the transaction isolation level to REPEATABLE READ before starting the transaction.
        setTransactionIsolationLevelRepeatableRead(mysql, sql);
        sql.set("START TRANSACTION WITH CONSISTENT SNAPSHOT");
        mysql.execute(sql.get());
    }

    /**
     * Read the current binglog position and set it in the source.
     *
     * @param source
     * @param mysql
     * @param sql
     * @throws SQLException
     */
    protected void readBinlogPosition(SourceInfo source,
                                      JdbcConnection mysql,
                                      AtomicReference<String> sql)
            throws SQLException {
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
                    logger.info("\tusing binlog '{}' at position '{}' and gtid '{}'", binlogFilename, binlogPosition,
                        gtidSet);
                } else {
                    logger.info("\tusing binlog '{}' at position '{}'", binlogFilename, binlogPosition);
                }
                source.startSnapshot();
                // ^^ I'm not a huge fan of this being tied up in here...
                // does it need to be? it seems like this will be called a lot, and really only needs to be called once?
            } else {
                throw new IllegalStateException("Cannot read the binlog filename and position via '" + showMasterStmt
                    + "'. Make sure your server is correctly configured");
            }
        });
    }

    private Collection<String> getDatabaseNames(JdbcConnection mysql, AtomicReference<String> sql)
            throws SQLException {
        final List<String> databaseNames = new ArrayList<>();
        sql.set("SHOW DATABASES");
        mysql.query(sql.get(), rs -> {
            while (rs.next()) {
                databaseNames.add(rs.getString(1));
            }
        });
        logger.info("\tlist of available databases is: {}", databaseNames);
        return databaseNames;
    }

    protected Collection<TableId> getTableNames(JdbcConnection mysql,
                                                AtomicReference<String> sql)
            throws SQLException {
        Collection<String> databaseNames = getDatabaseNames(mysql, sql);
        List<TableId> tableIds = new ArrayList<>();
        Filters filters = context.dbSchema().filters();
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
                            logger.info("\tincluding '{}'", id);
                        } else {
                            logger.info("\t'{}' is filtered out, discarding", id);
                        }
                    }
                });
                readableDatabaseNames.add(dbName);
            } catch (SQLException e) {
                // We were unable to execute the query or process the results, so skip this ...
                logger.warn("\tskipping database '{}' due to error reading tables: {}", dbName, e.getMessage());
            }
        }
        final Set<String> includedDatabaseNames = readableDatabaseNames.stream().filter(filters.databaseFilter()).collect(Collectors.toSet());
        logger.info("\tsnapshot continuing with database(s): {}", includedDatabaseNames);
        return tableIds;
    }

    protected class TableInfo {

        Collection<TableId> fullTableList;
        Map<String, List<TableId>> databaseToTableList;

        private TableInfo() {

        }


    }

    /**
     *
     * @param mysql
     * @param sql
     * @throws SQLException
     */
    protected void lockAllTables(JdbcConnection mysql, AtomicReference<String> sql) throws SQLException {
        sql.set("FLUSH TABLES WITH READ LOCK");
        mysql.execute(sql.get());
        metrics.globalLockAcquired();
    }

    /**
     * Lock a specific list of tables.
     *
     * @param tableIds the list of tables to lock
     * @param source
     * @param mysql
     * @param sql
     * @throws SQLException
     */
    protected void lockTableList(List<TableId> tableIds,
                                 SourceInfo source,
                                 JdbcConnection mysql,
                                 AtomicReference<String> sql)
            throws SQLException {
        if (!context.userHasPrivileges("LOCK TABLES")) {
            // We don't have the right privileges
            throw new ConnectException("User does not have the 'LOCK TABLES' privilege required to obtain a "
                + "consistent snapshot by preventing concurrent writes to tables.");
        }
        // We have the required privileges, so try to lock all of the tables we're interested in ...
        logger.info("\tFlush and obtain read lock for {} tables (preventing writes)", tableIds.size());
        String tableList = tableIds.stream()
            .map(this::quote)
            .collect(Collectors.joining(","));
        if (tableList != null) {
            sql.set("FLUSH TABLES " + tableList + " WITH READ LOCK");
            mysql.execute(sql.get());
        }
    }

    protected String quote(String dbOrTableName) {
        return "`" + dbOrTableName + "`";
    }

    protected String quote(TableId id) {
        return quote(id.catalog()) + "." + quote(id.table());
    }

}
