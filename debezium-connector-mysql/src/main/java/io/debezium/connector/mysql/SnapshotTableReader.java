package io.debezium.connector.mysql;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Class snapshotting an individual table
 * todo use a chainedReader to link all these individual readers together? maybe? is that what I want?
 * the chained reader will make sure all of an individual reader's sourceRecords are polled before starting a new one.
 * is that what I want?
 */
public class SnapshotTableReader extends AbstractReader {

    private TableId tableId;

    private volatile Thread thread;

    /**
     * Create a table snapshot reader.
     *
     * @param name    the name of the reader
     * @param context the task context in which this reader is running; may not be null
     */
    public SnapshotTableReader(TableId tableId, String name, MySqlTaskContext context) {
        super(name, context);
        this.tableId = tableId;
    }

    @Override
    protected void doStart() {
        thread = new Thread(this::execute, "mysql-snapshot-table-" + tableId.toString());
        thread.start();
    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doCleanup() {

    }

    private void execute() {

        // do snapshot
        // DON'T do follow-up binlog stuff. Lets not block on other snapshots.

        // this seems important
        final AtomicReference<String> sql = new AtomicReference<>();
        final JdbcConnection mysql = context.jdbc();


    }
}
