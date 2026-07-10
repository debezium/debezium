/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.sql.SQLException;

import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

/**
 * A signal-based incremental snapshot change event source for binlog-based connectors.
 * <p>
 * Binlog-based connectors build all table models by parsing DDL. When the schema of a snapshotted
 * table is missing from the schema history, the generic fallback reads it from JDBC
 * {@code DatabaseMetaData}, which yields a diverging model (debezium/dbz#1550); this subclass reads
 * the definition via {@code SHOW CREATE TABLE} and the connector's DDL parser instead, matching the
 * snapshot and streaming phases.
 */
public class BinlogSignalBasedIncrementalSnapshotChangeEventSource<P extends BinlogPartition>
        extends SignalBasedIncrementalSnapshotChangeEventSource<P, TableId> {

    private final BinlogDatabaseSchema<P, ?, ?, ?> schema;
    private final BinlogConnectorConnection binlogConnectorConnection;

    public BinlogSignalBasedIncrementalSnapshotChangeEventSource(BinlogConnectorConfig config,
                                                                 BinlogConnectorConnection jdbcConnection,
                                                                 EventDispatcher<P, TableId> dispatcher,
                                                                 BinlogDatabaseSchema<P, ?, ?, ?> databaseSchema,
                                                                 Clock clock,
                                                                 SnapshotProgressListener<P> progressListener,
                                                                 DataChangeEventListener<P> dataChangeEventListener,
                                                                 NotificationService<P, ? extends OffsetContext> notificationService) {
        super(config, jdbcConnection, dispatcher, databaseSchema, clock, progressListener, dataChangeEventListener, notificationService);
        this.schema = databaseSchema;
        this.binlogConnectorConnection = jdbcConnection;
    }

    @Override
    protected Table readSchemaForTable(TableId tableId) throws SQLException {
        return BinlogIncrementalSnapshotSchemaReader.readSchemaViaShowCreateTable(binlogConnectorConnection, schema, tableId);
    }
}
