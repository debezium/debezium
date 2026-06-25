/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

/**
 * Performs the initial snapshot of the ${connectorName} database.
 *
 * <p>Extends {@link RelationalSnapshotChangeEventSource}, which drives the whole snapshot
 * lifecycle: table enumeration, connection and transaction management, chunked reading, and
 * emitting one read ('r') record per row through the dispatcher. This class fills in only the
 * database-specific steps. The TODO methods are where most of the connector-specific work goes.
 */
public class ${connectorName}SnapshotChangeEventSource
        extends RelationalSnapshotChangeEventSource<${connectorName}Partition, ${connectorName}OffsetContext> {

    private final ${connectorName}ConnectorConfig connectorConfig;
    private final ${connectorName}Connection jdbcConnection;

    public ${connectorName}SnapshotChangeEventSource(${connectorName}ConnectorConfig connectorConfig,
                                                     MainConnectionProvidingConnectionFactory<${connectorName}Connection> connectionFactory,
                                                     ${connectorName}DatabaseSchema schema,
                                                     EventDispatcher<${connectorName}Partition, TableId> dispatcher,
                                                     Clock clock,
                                                     SnapshotProgressListener<${connectorName}Partition> snapshotProgressListener,
                                                     NotificationService<${connectorName}Partition, ${connectorName}OffsetContext> notificationService,
                                                     SnapshotterService snapshotterService) {
        super(connectorConfig, connectionFactory, schema, dispatcher, clock, snapshotProgressListener,
                notificationService, snapshotterService);
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = connectionFactory.mainConnection();
    }

    @Override
    protected SnapshotContext<${connectorName}Partition, ${connectorName}OffsetContext> prepare(
            ${connectorName}Partition partition, boolean onDemand) {
        String catalogName = connectorConfig.getJdbcConfig().getString(JdbcConfiguration.DATABASE);
        return new RelationalSnapshotContext<>(partition, catalogName, onDemand);
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<${connectorName}Partition, ${connectorName}OffsetContext> ctx)
            throws Exception {
        return jdbcConnection.readTableNames(ctx.catalogName, null, null, new String[]{ "TABLE" });
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSource.ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<${connectorName}Partition, ${connectorName}OffsetContext> snapshotContext)
            throws Exception {
        // TODO: lock the captured tables if your database needs to block concurrent DDL during the
        // schema snapshot. Many databases provide a consistent read view without locking, in which
        // case this can stay empty.
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<${connectorName}Partition, ${connectorName}OffsetContext> snapshotContext)
            throws Exception {
        // TODO: release whatever lockTablesForSchemaSnapshot acquired. Empty if no locks were taken.
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<${connectorName}Partition, ${connectorName}OffsetContext> ctx,
                                           ${connectorName}OffsetContext previousOffset)
            throws Exception {
        if (previousOffset != null && !snapshotterService.getSnapshotter().shouldStreamEventsStartingFromSnapshot()) {
            ctx.offset = previousOffset;
            return;
        }
        // TODO: read the current streaming position (LSN, change id, log offset, ...) from the same
        // read view the snapshot uses, and store it on the offset so streaming resumes without gaps.
        ctx.offset = new ${connectorName}OffsetContext(new ${connectorName}SourceInfo(connectorConfig));
    }

    @Override
    protected void readTableStructure(ChangeEventSource.ChangeEventSourceContext sourceContext,
                                      RelationalSnapshotContext<${connectorName}Partition, ${connectorName}OffsetContext> snapshotContext,
                                      ${connectorName}OffsetContext offsetContext, SnapshottingTask snapshottingTask)
            throws SQLException {
        jdbcConnection.readSchema(
                snapshotContext.tables,
                snapshotContext.catalogName,
                null,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                null,
                false);
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<${connectorName}Partition, ${connectorName}OffsetContext> snapshotContext,
                                                    Table table) {
        return SchemaChangeEvent.ofSnapshotCreate(snapshotContext.partition, snapshotContext.offset,
                snapshotContext.catalogName, table);
    }

    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<${connectorName}Partition, ${connectorName}OffsetContext> snapshotContext,
                                                 TableId tableId, List<String> columns) {
        return snapshotterService.getSnapshotQuery().snapshotQuery(jdbcConnection.quotedTableIdString(tableId), columns);
    }

    @Override
    protected ${connectorName}OffsetContext copyOffset(RelationalSnapshotContext<${connectorName}Partition, ${connectorName}OffsetContext> snapshotContext) {
        return new ${connectorName}OffsetLoader(connectorConfig).load(snapshotContext.offset.getOffset());
    }
}
