/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.common.SourceRecordWrapper;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.spi.SlotCreationResult;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.util.Clock;

public class PostgresSnapshotChangeEventSource<SourceRecord extends SourceRecordWrapper> extends RelationalSnapshotChangeEventSource<SourceRecord> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSnapshotChangeEventSource.class);

    private final PostgresConnectorConfig connectorConfig;
    private final PostgresConnection jdbcConnection;
    private final PostgresSchema schema;
    private final Snapshotter snapshotter;
    private final SlotCreationResult slotCreatedInfo;
    private final SlotState startingSlotInfo;
    private final PostgresOffsetContext previousOffset;

    public PostgresSnapshotChangeEventSource(PostgresConnectorConfig connectorConfig, Snapshotter snapshotter, PostgresOffsetContext previousOffset,
                                             PostgresConnection jdbcConnection, PostgresSchema schema, EventDispatcher<TableId, SourceRecord> dispatcher, Clock clock,
                                             SnapshotProgressListener snapshotProgressListener, SlotCreationResult slotCreatedInfo, SlotState startingSlotInfo) {
        super(connectorConfig, previousOffset, jdbcConnection, dispatcher, clock, snapshotProgressListener);
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = jdbcConnection;
        this.schema = schema;
        this.snapshotter = snapshotter;
        this.slotCreatedInfo = slotCreatedInfo;
        this.startingSlotInfo = startingSlotInfo;
        this.previousOffset = previousOffset;
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(OffsetContext previousOffset) {
        boolean snapshotSchema = true;
        boolean snapshotData = true;

        snapshotData = snapshotter.shouldSnapshot();
        if (snapshotData) {
            LOGGER.info("According to the connector configuration data will be snapshotted");
        }
        else {
            LOGGER.info("According to the connector configuration no snapshot will be executed");
            snapshotSchema = false;
        }

        return new SnapshottingTask(snapshotSchema, snapshotData);
    }

    @Override
    protected SnapshotContext prepare(ChangeEventSourceContext context) throws Exception {
        return new PostgresSnapshotContext(connectorConfig.databaseName());
    }

    @Override
    protected void connectionCreated(RelationalSnapshotContext snapshotContext) throws Exception {
        // If using catch up streaming, the connector opens the transaction that the snapshot will eventually use
        // before the catch up streaming starts. By looking at the current wal location, the transaction can determine
        // where the catch up streaming should stop. The transaction is held open throughout the catch up
        // streaming phase so that the snapshot is performed from a consistent view of the data. Since the isolation
        // level on the transaction used in catch up streaming has already set the isolation level and executed
        // statements, the transaction does not need to get set the level again here.
        if (snapshotter.shouldStreamEventsStartingFromSnapshot() && startingSlotInfo == null) {
            setSnapshotTransactionIsolationLevel();
        }
        schema.refresh(jdbcConnection, false);
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext ctx) throws Exception {
        return jdbcConnection.readTableNames(ctx.catalogName, null, null, new String[]{ "TABLE" });
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext, RelationalSnapshotContext snapshotContext)
            throws SQLException, InterruptedException {
        final Duration lockTimeout = connectorConfig.snapshotLockTimeout();
        final Optional<String> lockStatement = snapshotter.snapshotTableLockingStatement(lockTimeout, snapshotContext.capturedTables);

        if (lockStatement.isPresent()) {
            LOGGER.info("Waiting a maximum of '{}' seconds for each table lock", lockTimeout.getSeconds());
            jdbcConnection.executeWithoutCommitting(lockStatement.get());
            // now that we have the locks, refresh the schema
            schema.refresh(jdbcConnection, false);
        }
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext snapshotContext) throws SQLException {
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext ctx) throws Exception {
        PostgresOffsetContext offset = (PostgresOffsetContext) ctx.offset;
        if (offset == null) {
            if (previousOffset != null && !snapshotter.shouldStreamEventsStartingFromSnapshot()) {
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
        final long txId = jdbcConnection.currentTransactionId().longValue();
        LOGGER.info("Read xlogStart at '{}' from transaction '{}'", xlogStart, txId);

        // use the old xmin, as we don't want to update it if in xmin recovery
        offset.updateWalPosition(xlogStart, offset.lastCompletelyProcessedLsn(), clock.currentTime(), txId, null, offset.xmin());
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
        else if (!snapshotter.shouldStreamEventsStartingFromSnapshot() && startingSlotInfo != null) {
            // Allow streaming to resume from where streaming stopped last rather than where the current snapshot starts.
            SlotState currentSlotState = jdbcConnection.getReplicationSlotState(connectorConfig.slotName(),
                    connectorConfig.plugin().getPostgresPluginName());
            return currentSlotState.slotLastFlushedLsn();
        }

        return Lsn.valueOf(jdbcConnection.currentXLogLocation());
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext, RelationalSnapshotContext snapshotContext) throws SQLException, InterruptedException {
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

            LOGGER.info("Reading structure of schema '{}'", snapshotContext.catalogName);
            jdbcConnection.readSchema(
                    snapshotContext.tables,
                    snapshotContext.catalogName,
                    schema,
                    connectorConfig.getTableFilters().dataCollectionFilter(),
                    null,
                    false);
        }
        schema.refresh(jdbcConnection, false);
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext snapshotContext, Table table) throws SQLException {
        return new SchemaChangeEvent(
                snapshotContext.offset.getPartition(),
                snapshotContext.offset.getOffset(),
                snapshotContext.offset.getSourceInfo(),
                snapshotContext.catalogName,
                table.id().schema(),
                null,
                table,
                SchemaChangeEventType.CREATE,
                true);
    }

    @Override
    protected void complete(SnapshotContext snapshotContext) {
        snapshotter.snapshotCompleted();
    }

    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext snapshotContext, TableId tableId) {
        return snapshotter.buildSnapshotQuery(tableId);
    }

    @Override
    protected Object getColumnValue(ResultSet rs, int columnIndex, Column column, Table table) throws SQLException {
        return jdbcConnection.getColumnValue(rs, columnIndex, column, table, schema);
    }

    protected void setSnapshotTransactionIsolationLevel() throws SQLException {
        LOGGER.info("Setting isolation level");
        String transactionStatement = snapshotter.snapshotTransactionIsolationLevelStatement(slotCreatedInfo);
        LOGGER.info("Opening transaction with statement {}", transactionStatement);
        jdbcConnection.executeWithoutCommitting(transactionStatement);
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private static class PostgresSnapshotContext extends RelationalSnapshotContext {

        public PostgresSnapshotContext(String catalogName) throws SQLException {
            super(catalogName);
        }
    }
}
