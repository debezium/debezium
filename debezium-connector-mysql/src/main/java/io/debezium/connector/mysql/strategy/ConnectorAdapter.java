/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.strategy;

import java.util.Map;

import com.github.shyiko.mysql.binlog.event.EventData;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlDatabaseSchema;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;

/**
 * Provides the MySQL connector with an adapter pattern to support varied configurations
 * between MySQL and MariaDB and their drivers.
 *
 * @author Chris Cranford
 */
public interface ConnectorAdapter {

    // todo: we had to introduce the configuration argument because of the extra database properties
    // that are set in the task; we would get a test failure because of those missing if we
    // simply created the connection configuration the base data in the MySqlConnectorConfig ctor
    AbstractConnectorConnection createConnection(Configuration configuration);

    BinaryLogClientConfigurator getBinaryLogClientConfigurator();

    /**
     * Sets the offset context binlog details.
     *
     * @param offsetContext the offset context to be mutated
     * @param connection the database connection to be used
     * @throws Exception if an exception is thrown
     */
    void setOffsetContextBinlogPositionAndGtidDetailsForSnapshot(MySqlOffsetContext offsetContext,
                                                                 AbstractConnectorConnection connection)
            throws Exception;

    // todo: should we consider splitting value converters, it may prove useful in the future
    // doing so would imply we won't likely need this method as it can be encapsulated?
    String getJavaEncodingForCharSet(String charSetName);

    // todo: for the moment we only expose the few handler deviations
    // we may want to simply implement an abstract and concrete streaming impls

    String getRecordingQueryFromEvent(EventData event);

    AbstractHistoryRecordComparator getHistoryRecordComparator();

    <T> IncrementalSnapshotContext<T> getIncrementalSnapshotContext();

    <T> IncrementalSnapshotContext<T> loadIncrementalSnapshotContextFromOffset(Map<String, ?> offset);

    Long getReadOnlyIncrementalSnapshotSignalOffset(MySqlOffsetContext previousOffsets);

    IncrementalSnapshotChangeEventSource<MySqlPartition, ? extends DataCollectionId> createIncrementalSnapshotChangeEventSource(
                                                                                                                                MySqlConnectorConfig connectorConfig,
                                                                                                                                AbstractConnectorConnection connection,
                                                                                                                                EventDispatcher<MySqlPartition, ? extends DataCollectionId> dispatcher,
                                                                                                                                MySqlDatabaseSchema schema,
                                                                                                                                Clock clock,
                                                                                                                                SnapshotProgressListener<MySqlPartition> snapshotProgressListener,
                                                                                                                                DataChangeEventListener<MySqlPartition> dataChangeEventListener,
                                                                                                                                NotificationService<MySqlPartition, MySqlOffsetContext> notificationService);
}
