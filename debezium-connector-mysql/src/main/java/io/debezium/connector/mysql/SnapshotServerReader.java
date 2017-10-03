/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.function.BufferedBlockingConsumer;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

/**
 * A component that performs a snapshot of a MySQL server, and records the schema changes in {@link MySqlSchema}.
 *
 * @author Randall Hauch
 */
public class SnapshotServerReader extends AbstractSnapshotReader {

    private final boolean includeData;
    private final SnapshotReaderMetrics metrics;
    private final SnapshotMySQLUtility snapshotMySQLUtility;

    /**
     * Create a snapshot reader.
     *
     * @param name the name of this reader; may not be null
     * @param context the task context in which this reader is running; may not be null
     */
    public SnapshotServerReader(String name, MySqlTaskContext context) {
        super(name, context);
        this.includeData = !context.isSchemaOnlySnapshot();
        metrics = new SnapshotReaderMetrics(context.clock());
        metrics.register(context, logger);
        snapshotMySQLUtility = new SnapshotMySQLUtility(context);
    }

    /**
     * Perform the snapshot using the same logic as the "mysqldump" utility.
     */
    protected void execute() {
        context.configureLoggingContext("snapshot");
        final AtomicReference<String> sql = new AtomicReference<>();
        final JdbcConnection mysql = context.jdbc();
        final MySqlSchema schema = context.dbSchema();
        final SourceInfo source = context.source();
        final Clock clock = context.clock();
        final long ts = clock.currentTimeInMillis();
        logger.info("Starting snapshot for {} with user '{}'", context.connectionString(), mysql.username());
        logRolesForCurrentUser(mysql);
        logServerInformation(mysql);
        try {
            metrics.startSnapshot();

            try {
                // I can't help but feel like there's got to be a better way to do this.
                if (!isRunning()) return;

                snapshotMySQLUtility.lockAllTables();
                snapshotMySQLUtility.beginTransaction();

                source.setBinlogStartPoint(snapshotMySQLUtility.getBinlogFilename(), snapshotMySQLUtility.getBinlogPosition());
                source.setCompletedGtidSet(snapshotMySQLUtility.getGtidSet());

                if (!isRunning()) return;

                if (!snapshotMySQLUtility.hasReadTables()) {
                    snapshotMySQLUtility.readTableList();
                }

                List<TableId> tableIds = snapshotMySQLUtility.getTableIds();
                Set<String> readableDatabaseNames = snapshotMySQLUtility.getReadableDatabaseNames();

                // From this point forward, all source records produced by this connector will have an offset that includes a
                // "snapshot" field (with value of "true").

                // Transform the current schema so that it reflects the *current* state of the MySQL server's contents.
                // First, get the DROP TABLE and CREATE TABLE statement (with keys and constraint definitions) for our tables ...
                // Generate the DDL statements that set the charset-related system variables ...
                logger.info("Generating DROP and CREATE statements to reflect current database schemas:");
                // todo figure out what this is for/where it needs to be in snapshotmysqlutility
                String setSystemVariablesStatement = context.setStatementFor(context.readMySqlCharsetSystemVariables(sql));
                schema.applyDdl(source, null, setSystemVariablesStatement, this::enqueueSchemaChanges);

                // Add DROP TABLE statements for all tables that we knew about AND those tables found in the databases...
                Set<TableId> allTableIds = new HashSet<>(schema.tables().tableIds());
                allTableIds.addAll(tableIds);
                snapshotMySQLUtility.createDropTableEvents(allTableIds, this::enqueueSchemaChanges);

                Set<String> allDatabases = schema.tables().tableIds().stream().map(TableId::catalog).collect(Collectors.toSet());
                allDatabases.addAll(readableDatabaseNames);
                // drop all databases
                snapshotMySQLUtility.createDropDatabaseEvents(allDatabases, this::enqueueSchemaChanges);
                //re-create databases
                snapshotMySQLUtility.createCreateDatabaseEvents(readableDatabaseNames, this::enqueueSchemaChanges);
                //create tables
                snapshotMySQLUtility.createCreateTableEvents(tableIds, this::enqueueSchemaChanges);

                context.makeRecord().regenerate();

                if (!isRunning()) return;
                if (includeData) {
                    BufferedBlockingConsumer<SourceRecord> bufferedRecordQueue = BufferedBlockingConsumer.bufferLast(super::enqueueRecord);
                    snapshotMySQLUtility.bufferLockedTables(recorder, bufferedRecordQueue);
                } else {
                    // source.markLastSnapshot(); Think we will not be needing this here it is used to mark last row entry?
                    logger.info("Encountered only schema based snapshot, skipping data snapshot");
                }
            } finally {
                snapshotMySQLUtility.completeTransactionAndUnlockTables();
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
                // Mark the source as having completed the snapshot. This will ensure the `source` field on records
                // are not denoted as a snapshot ...
                source.completeSnapshot();
                // Set the completion flag ...
                completeSuccessfully();
                long stop = clock.currentTimeInMillis();
                logger.info("Completed snapshot in {}", Strings.duration(stop - ts));
            }
        } catch (Throwable e) {
            failed(e, "Aborting snapshot due to error when last running '" + sql.get() + "': " + e.getMessage());
        }
    }
}
