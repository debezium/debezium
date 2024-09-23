/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.PostgresOffsetContext.Loader;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.spi.SlotCreationResult;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;

public class PostgresSnapshotChangeEventSource extends RelationalSnapshotChangeEventSource<PostgresPartition, PostgresOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSnapshotChangeEventSource.class);

    private final PostgresConnectorConfig connectorConfig;
    private final PostgresConnection jdbcConnection;
    private final PostgresSchema schema;
    private final SlotCreationResult slotCreatedInfo;
    private final SlotState startingSlotInfo;

    public PostgresSnapshotChangeEventSource(PostgresConnectorConfig connectorConfig, SnapshotterService snapshotterService,
                                             MainConnectionProvidingConnectionFactory<PostgresConnection> connectionFactory, PostgresSchema schema,
                                             EventDispatcher<PostgresPartition, TableId> dispatcher, Clock clock,
                                             SnapshotProgressListener<PostgresPartition> snapshotProgressListener, SlotCreationResult slotCreatedInfo,
                                             SlotState startingSlotInfo, NotificationService<PostgresPartition, PostgresOffsetContext> notificationService) {
        super(connectorConfig, connectionFactory, schema, dispatcher, clock, snapshotProgressListener, notificationService, snapshotterService);
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = connectionFactory.mainConnection();
        this.schema = schema;
        this.slotCreatedInfo = slotCreatedInfo;
        this.startingSlotInfo = startingSlotInfo;
    }

    @Override
    public SnapshottingTask getSnapshottingTask(PostgresPartition partition, PostgresOffsetContext previousOffset) {

        boolean snapshotSchema = true;

        List<String> dataCollectionsToBeSnapshotted = connectorConfig.getDataCollectionsToBeSnapshotted();
        Map<DataCollectionId, String> snapshotSelectOverridesByTable = connectorConfig.getSnapshotSelectOverridesByTable();

        boolean offsetExists = previousOffset != null;
        boolean snapshotInProgress = false;

        if (offsetExists) {
            snapshotInProgress = previousOffset.isSnapshotRunning();
        }

        if (offsetExists && !previousOffset.isSnapshotRunning()) {
            LOGGER.info("A previous offset indicating a completed snapshot has been found.");
        }

        boolean snapshotData = snapshotterService.getSnapshotter().shouldSnapshotData(offsetExists, snapshotInProgress);
        if (snapshotData) {
            LOGGER.info("According to the connector configuration data will be snapshotted");
        }
        else {
            LOGGER.info("According to the connector configuration no snapshot will be executed");
            snapshotSchema = false;
        }

        return new SnapshottingTask(snapshotSchema, snapshotData, dataCollectionsToBeSnapshotted, snapshotSelectOverridesByTable, false);
    }

    @Override
    protected SnapshotContext<PostgresPartition, PostgresOffsetContext> prepare(PostgresPartition partition, boolean onDemand) {
        return new PostgresSnapshotContext(partition, connectorConfig.databaseName(), onDemand);
    }

    @Override
    protected void connectionCreated(RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext)
            throws Exception {
        // If using catch up streaming, the connector opens the transaction that the snapshot will eventually use
        // before the catch up streaming starts. By looking at the current wal location, the transaction can determine
        // where the catch up streaming should stop. The transaction is held open throughout the catch up
        // streaming phase so that the snapshot is performed from a consistent view of the data. Since the isolation
        // level on the transaction used in catch up streaming has already set the isolation level and executed
        // statements, the transaction does not need to get set the level again here.
        if (snapshotterService.getSnapshotter().shouldStreamEventsStartingFromSnapshot() && startingSlotInfo == null) {
            setSnapshotTransactionIsolationLevel(snapshotContext.onDemand);
        }
        schema.refresh(jdbcConnection, false);
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> ctx)
            throws Exception {
        return jdbcConnection.getAllTableIds(ctx.catalogName);
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext)
            throws SQLException {

        final Duration lockTimeout = connectorConfig.snapshotLockTimeout();
        final Set<String> capturedTablesNames = snapshotContext.capturedTables.stream().map(TableId::toDoubleQuotedString).collect(Collectors.toSet());

        List<String> tableLockStatements = capturedTablesNames.stream()
                .map(tableId -> snapshotterService.getSnapshotLock().tableLockingStatement(lockTimeout, tableId))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        if (!tableLockStatements.isEmpty()) {

            String lineSeparator = System.lineSeparator();
            StringBuilder statements = new StringBuilder();
            statements.append("SET lock_timeout = ").append(lockTimeout.toMillis()).append(";").append(lineSeparator);
            // we're locking in ACCESS SHARE MODE to avoid concurrent schema changes while we're taking the snapshot
            // this does not prevent writes to the table, but prevents changes to the table's schema....
            // DBZ-298 Quoting name in case it has been quoted originally; it doesn't do harm if it hasn't been quoted
            tableLockStatements.forEach(tableStatement -> statements.append(tableStatement).append(lineSeparator));

            LOGGER.info("Waiting a maximum of '{}' seconds for each table lock", lockTimeout.getSeconds());
            jdbcConnection.executeWithoutCommitting(statements.toString());
            // now that we have the locks, refresh the schema
            schema.refresh(jdbcConnection, false);
        }
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext)
            throws SQLException {
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> ctx, PostgresOffsetContext previousOffset)
            throws Exception {
        PostgresOffsetContext offset = ctx.offset;
        if (offset == null) {
            if (previousOffset != null && !snapshotterService.getSnapshotter().shouldStreamEventsStartingFromSnapshot()) {
                // The connect framework, not the connector, manages triggering committing offset state so the
                // replication stream may not have flushed the latest offset state during catch up streaming.
                // The previousOffset variable is shared between the catch up streaming and snapshot phases and
                // has the latest known offset state.
                offset = PostgresOffsetContext.initialContext(connectorConfig, jdbcConnection, getClock(),
                        previousOffset.lastCommitLsn(), previousOffset.lastCompletelyProcessedLsn());
            }
            else {
                offset = PostgresOffsetContext.initialContext(connectorConfig, jdbcConnection, getClock());
            }
            ctx.offset = offset;
        }

        updateOffsetForSnapshot(offset);
    }

    private void updateOffsetForSnapshot(PostgresOffsetContext offset) throws SQLException {
        final Lsn xlogStart = getTransactionStartLsn();
        final Long txId = jdbcConnection.currentTransactionId();
        LOGGER.info("Read xlogStart at '{}' from transaction '{}'", xlogStart, txId);

        // use the old xmin, as we don't want to update it if in xmin recovery
        offset.updateWalPosition(xlogStart, offset.lastCompletelyProcessedLsn(), clock.currentTime(), txId, offset.xmin(), null, null);
    }

    protected void updateOffsetForPreSnapshotCatchUpStreaming(PostgresOffsetContext offset) throws SQLException {
        updateOffsetForSnapshot(offset);
        offset.setStreamingStoppingLsn(Lsn.valueOf(jdbcConnection.currentXLogLocation()));
    }

    private Lsn getTransactionStartLsn() throws SQLException {
        if (slotCreatedInfo != null) {
            // When performing an exported snapshot based on a newly created replication slot, the txLogStart position
            // should be based on the replication slot snapshot transaction point. This is crucial so that if any
            // SQL operations occur mid-snapshot that they'll be properly captured when streaming begins; otherwise
            // they'll be lost.
            return slotCreatedInfo.startLsn();
        }
        else if (!snapshotterService.getSnapshotter().shouldStreamEventsStartingFromSnapshot() && startingSlotInfo != null) {
            // Allow streaming to resume from where streaming stopped last rather than where the current snapshot starts.
            SlotState currentSlotState = jdbcConnection.getReplicationSlotState(connectorConfig.slotName(),
                    connectorConfig.plugin().getPostgresPluginName());
            return currentSlotState.slotLastFlushedLsn();
        }

        return Lsn.valueOf(jdbcConnection.currentXLogLocation());
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext,
                                      RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext,
                                      PostgresOffsetContext offsetContext, SnapshottingTask snapshottingTask)
            throws SQLException, InterruptedException {
        Set<String> schemas = snapshotContext.capturedTables.stream()
                .map(TableId::schema)
                .collect(Collectors.toSet());

        // reading info only for the schemas we're interested in as per the set of captured tables;
        // while the passed table name filter alone would skip all non-included tables, reading the schema
        // would take much longer that way
        for (String schema : schemas) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while reading structure of schema " + schema);
            }

            LOGGER.info("Reading structure of schema '{}' of catalog '{}'", schema, snapshotContext.catalogName);

            Tables.TableFilter tableFilter = snapshottingTask.isOnDemand() ? Tables.TableFilter.fromPredicate(snapshotContext.capturedTables::contains)
                    : connectorConfig.getTableFilters().dataCollectionFilter();

            jdbcConnection.readSchema(
                    snapshotContext.tables,
                    snapshotContext.catalogName,
                    schema,
                    tableFilter,
                    null,
                    false);
        }
        schema.refresh(jdbcConnection, false);
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext,
                                                    Table table) {
        return SchemaChangeEvent.ofSnapshotCreate(snapshotContext.partition, snapshotContext.offset, snapshotContext.catalogName, table);
    }

    @Override
    protected void completed(SnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext) {
        snapshotterService.getSnapshotter().snapshotCompleted();
    }

    @Override
    protected void aborted(SnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext) {
        snapshotterService.getSnapshotter().snapshotAborted();
    }

    /**
     * Generate a valid Postgres query string for the specified table and columns
     *
     * @param tableId the table to generate a query for
     * @return a valid query string
     */
    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext,
                                                 TableId tableId, List<String> columns) {

        return snapshotterService.getSnapshotQuery().snapshotQuery(tableId.toDoubleQuotedString(), columns);
    }

    protected void setSnapshotTransactionIsolationLevel(boolean isOnDemand) throws SQLException {
        LOGGER.info("Setting isolation level");
        String transactionStatement = snapshotTransactionIsolationLevelStatement(slotCreatedInfo, isOnDemand);
        LOGGER.info("Opening transaction with statement {}", transactionStatement);
        jdbcConnection.executeWithoutCommitting(transactionStatement);
    }

    private String snapshotTransactionIsolationLevelStatement(SlotCreationResult newSlotInfo, boolean isOnDemand) {

        if (newSlotInfo != null && !isOnDemand) {
            /*
             * For an on demand blocking snapshot we don't need to reuse
             * the same snapshot from the existing exported transaction as for the initial snapshot.
             */
            String snapSet = String.format("SET TRANSACTION SNAPSHOT '%s';", newSlotInfo.snapshotName());
            return "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; \n" + snapSet;
        }

        // DEFERRABLE only takes affect for READY ONLY and SERIALIZABLE
        // https://www.postgresql.org/docs/current/sql-set-transaction.html
        String transactionIsolationLevelStatement = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE;";
        switch (connectorConfig.getSnapshotIsolationMode()) {
            case REPEATABLE_READ:
                transactionIsolationLevelStatement = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;";
                break;
            case READ_COMMITTED:
                transactionIsolationLevelStatement = "SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY;";
                break;
            case READ_UNCOMMITTED:
                transactionIsolationLevelStatement = "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED, READ ONLY;";
                break;
            default:
                break;
        }
        return transactionIsolationLevelStatement;
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private static class PostgresSnapshotContext extends RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> {

        PostgresSnapshotContext(PostgresPartition partition, String catalogName, boolean onDemand) {
            super(partition, catalogName, onDemand);
        }
    }

    @Override
    protected PostgresOffsetContext copyOffset(RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext) {
        return new Loader(connectorConfig).load(snapshotContext.offset.getOffset());
    }
}
