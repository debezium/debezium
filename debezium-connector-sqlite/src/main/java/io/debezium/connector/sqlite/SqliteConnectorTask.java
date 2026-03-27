/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.util.Clock;

/**
 * Kafka Connect source task for the SQLite connector.
 * <p>
 * Manages the lifecycle of the {@link ChangeEventSourceCoordinator}, which orchestrates
 * the snapshot and streaming phases.
 *
 * @author Zihan Dai
 */
public class SqliteConnectorTask extends BaseSourceTask<SqlitePartition, SqliteOffsetContext> {

    private volatile ChangeEventSourceCoordinator<SqlitePartition, SqliteOffsetContext> coordinator;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public ChangeEventSourceCoordinator<SqlitePartition, SqliteOffsetContext> start(Configuration config) {
        // TODO: Initialize schema, connections, and start the coordinator.
        // This skeleton demonstrates the correct lifecycle structure.
        // Full implementation during GSoC coding period.
        final SqliteConnectorConfig connectorConfig = new SqliteConnectorConfig(config);

        // Placeholder: coordinator creation will follow the PostgreSQL/SQL Server pattern:
        // 1. Build SqliteDatabaseSchema (HistorizedRelationalDatabaseSchema)
        // 2. Create SqliteChangeEventSourceFactory (snapshot + streaming sources)
        // 3. Create and start ChangeEventSourceCoordinator

        return null; // TODO: return initialized coordinator
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        final List<DataChangeEvent> records = queue.poll();
        return records.stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());
    }

    @Override
    protected void doStop() {
        if (coordinator != null) {
            coordinator.stop();
        }
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return SqliteConnectorConfig.ALL_FIELDS;
    }

    private ChangeEventQueue<DataChangeEvent> queue;
}
