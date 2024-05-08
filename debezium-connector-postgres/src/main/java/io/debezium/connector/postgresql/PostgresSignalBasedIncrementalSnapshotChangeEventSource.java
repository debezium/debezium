/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.Clock;

/**
 * Custom PostgreSQL implementation of the {@link SignalBasedIncrementalSnapshotChangeEventSource} implementation
 * which performs an explicit schema refresh of a table prior to the incremental snapshot starting.
 *
 * @author Chris Cranford
 */
public class PostgresSignalBasedIncrementalSnapshotChangeEventSource
        extends SignalBasedIncrementalSnapshotChangeEventSource<PostgresPartition, TableId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSignalBasedIncrementalSnapshotChangeEventSource.class);

    private final PostgresConnection jdbcConnection;
    private final PostgresSchema schema;

    public PostgresSignalBasedIncrementalSnapshotChangeEventSource(RelationalDatabaseConnectorConfig config,
                                                                   JdbcConnection jdbcConnection,
                                                                   EventDispatcher<PostgresPartition, TableId> dispatcher,
                                                                   DatabaseSchema<?> databaseSchema,
                                                                   Clock clock,
                                                                   SnapshotProgressListener<PostgresPartition> progressListener,
                                                                   DataChangeEventListener<PostgresPartition> dataChangeEventListener,
                                                                   NotificationService<PostgresPartition, ? extends OffsetContext> notificationService) {
        super(config, jdbcConnection, dispatcher, databaseSchema, clock, progressListener, dataChangeEventListener, notificationService);
        this.jdbcConnection = (PostgresConnection) jdbcConnection;
        this.schema = (PostgresSchema) databaseSchema;
    }

    @Override
    protected Table refreshTableSchema(Table table) throws SQLException {
        LOGGER.debug("Refreshing table '{}' schema for incremental snapshot.", table.id());
        schema.refresh(jdbcConnection, table.id(), true);
        return schema.tableFor(table.id());
    }
}
