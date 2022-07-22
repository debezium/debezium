/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.notification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.pipeline.source.snapshot.incremental.DataCollection;
import io.debezium.spi.schema.DataCollectionId;

public class LogSnapshotStatusNotifications<T extends DataCollectionId> implements SnapshotStatusNotifications<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogSnapshotStatusNotifications.class);

    @Override
    public void configure(Configuration config) {

    }

    @Override
    public void snapshotCompleted(String connectorName, DataCollection<T> dataCollection, SnapshotCompletionStatus status) {
        LOGGER.info("Snapshot of the {} completed on connector {}", dataCollection, connectorName);
    }

}
