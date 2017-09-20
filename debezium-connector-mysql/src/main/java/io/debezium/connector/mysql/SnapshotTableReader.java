package io.debezium.connector.mysql;

import io.debezium.function.BufferedBlockingConsumer;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import org.apache.kafka.connect.source.SourceRecord;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Class snapshotting an individual table
 * todo use a chainedReader to link all these individual readers together? maybe? is that what I want?
 * the chained reader will make sure all of an individual reader's sourceRecords are polled before starting a new one.
 * is that what I want?
 */
public class SnapshotTableReader extends AbstractSnapshotReader {

    private TableId tableId;
    private SnapshotMySQLUtility snapshotMySQLUtility;

    /**
     * Create a table snapshot reader.
     *
     * @param name    the name of the reader
     * @param context the task context in which this reader is running; may not be null
     */
    public SnapshotTableReader(TableId tableId, String name, MySqlTaskContext context) {
        super(name, context);
        this.tableId = tableId;
        this.snapshotMySQLUtility = new SnapshotMySQLUtility(context);
    }

    protected void execute() {
        // do snapshot
        // DON'T do follow-up binlog stuff. Lets not block on other snapshots.
        context.configureLoggingContext("snapshot"); // tbh I don't actully know what this does.
        final AtomicReference<String> sql = new AtomicReference<>();
        final JdbcConnection mysql = context.jdbc();
        final MySqlSchema schema = context.dbSchema();
        final SourceInfo source = context.source();
        final Clock clock = context.clock();
        final long ts = clock.currentTimeInMillis();
        logger.info("Starting snapshot for {} with user '{}'", context.connectionString(), mysql.username());
        //logRolesForCurrentUser(mysql);
        //logServerInformation(mysql);

        try {
            metrics.startSnapshot();
            try {
                boolean globalLock = false;
                if (minimalBlocking) {
                    // if we globally lock, we can release the lock earlier.
                    try {
                        snapshotMySQLUtility.lockGlobal();
                        globalLock = true;
                    } catch (SQLException ex) {
                        // can't globally lock, instead just lock the single table
                        logger.info("Can't globally lock, locking single table instead.");
                    }

                }

                if (!globalLock) {
                    snapshotMySQLUtility.lockTables(Collections.singleton(tableId));
                }

                snapshotMySQLUtility.beginTransaction();

                if (globalLock) {
                    // read the table list.
                    // if we locked an individual table we have already read the table list
                    snapshotMySQLUtility.readTableList();
                }

                List<TableId> allTableIds = snapshotMySQLUtility.getTableIds();

                if (!allTableIds.contains(tableId)) {
                    // sanity check
                    throw new IllegalArgumentException("Table cannot be snapshotted because it does not exist");
                }

                // todo delete table before hand? delete/create database?
                snapshotMySQLUtility.createCreateTableEvents(Collections.singleton(tableId), this::enqueueSchemaChanges);

                context.makeRecord().regenerate();

                boolean includeData = !context.isSchemaOnlySnapshot();
                if (includeData) {
                    BufferedBlockingConsumer<SourceRecord> bufferedRecordQueue = BufferedBlockingConsumer.bufferLast(super::enqueueRecord);
                    snapshotMySQLUtility.bufferLockedTables(Collections.singletonList(tableId), recorder, bufferedRecordQueue);
                } else {
                    logger.info("Encountered only schema based snapshot, skipping data snapshot for table: " + tableId.toString());
                }
            } finally {
                snapshotMySQLUtility.completeTransactionAndUnlockTables();
            }

            // todo verify success?
            cleanupResources();
            completeSuccessfully();

        } catch (Throwable throwable) {
            failed(throwable, "Aborting snapshot due to error when last running '" + sql.get() + "': " + throwable.getMessage());
        }
    }
}
