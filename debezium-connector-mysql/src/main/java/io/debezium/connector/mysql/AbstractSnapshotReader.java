package io.debezium.connector.mysql;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.Strings;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract Snapshot Reader
 *
 */
public abstract class AbstractSnapshotReader extends AbstractReader {

    protected boolean minimalBlocking = true;
    protected RecordRecorder recorder;
    protected volatile Thread thread;
    protected final SnapshotReaderMetrics metrics;

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
        recorder = this::recordRowAsRead;
    }

    /**
     * Set whether this reader's {@link #execute()} should block other transactions as minimally as possible by
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
     * Set this reader's {@link #execute() execution} to produce a {@link io.debezium.data.Envelope.Operation#READ} event for each
     * row.
     *
     * @return this object for method chaining; never null
     */
    public AbstractSnapshotReader generateReadEvents() {
        recorder = this::recordRowAsRead;
        return this;
    }

    /**
     * Set this reader's {@link #execute() execution} to produce a {@link io.debezium.data.Envelope.Operation#CREATE} event for
     * each row.
     *
     * @return this object for method chaining; never null
     */
    public AbstractSnapshotReader generateInsertEvents() {
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

    protected void logServerInformation(JdbcConnection mysql) {
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

    protected void logRolesForCurrentUser(JdbcConnection mysql) {
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

    protected abstract void execute();

    private void recordRowAsRead(RecordMakers.RecordsForTable recordMaker, Object[] row, long ts) throws InterruptedException {
        recordMaker.read(row, ts);
    }

    private void recordRowAsInsert(RecordMakers.RecordsForTable recordMaker, Object[] row, long ts) throws InterruptedException {
        recordMaker.create(row, ts);
    }

    protected static interface RecordRecorder {
        void recordRow(RecordMakers.RecordsForTable recordMaker, Object[] row, long ts) throws InterruptedException;
    }

    protected void enqueueSchemaChanges(String dbName, String ddlStatement) {
        if (!context.includeSchemaChangeRecords() || ddlStatement.length() == 0) {
            return;
        }
        if (context.makeRecord().schemaChanges(dbName, ddlStatement, super::enqueueRecord) > 0) {
            logger.info("\t{}", ddlStatement);
        }
    }
}
