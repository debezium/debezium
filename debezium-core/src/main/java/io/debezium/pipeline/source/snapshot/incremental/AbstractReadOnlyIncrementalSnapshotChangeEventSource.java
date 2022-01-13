/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.NotThreadSafe;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.signal.AbstractExternalSignalThread;
import io.debezium.pipeline.signal.ExternalSignalThread;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.Clock;

/**
 * An read only incremental snapshot change event source that emits events from a DB log interleaved with snapshot events.
 */
@NotThreadSafe
public abstract class AbstractReadOnlyIncrementalSnapshotChangeEventSource<T extends DataCollectionId> extends AbstractIncrementalSnapshotChangeEventSource<T> {

    protected final ExternalSignalThread externalSignal;
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractReadOnlyIncrementalSnapshotChangeEventSource.class);

    public AbstractReadOnlyIncrementalSnapshotChangeEventSource(RelationalDatabaseConnectorConfig config,
                                                                JdbcConnection jdbcConnection,
                                                                EventDispatcher<T> dispatcher, DatabaseSchema<?> databaseSchema,
                                                                Clock clock, SnapshotProgressListener progressListener,
                                                                DataChangeEventListener dataChangeEventListener) {
        super(config, jdbcConnection, dispatcher, databaseSchema, clock, progressListener, dataChangeEventListener);
        final String externalSignalClassName = config.getConfig().getString("signal.class", "io.debezium.pipeline.signal.KafkaSignalThread");
        LOGGER.info("Using {} class for external signaling", externalSignalClassName);

        try {
            @SuppressWarnings("unchecked")
            Class<? extends AbstractExternalSignalThread<T>> connectorClass = (Class<AbstractExternalSignalThread<T>>) getClass().getClassLoader()
                    .loadClass(externalSignalClassName);
            externalSignal = connectorClass.getDeclaredConstructor(CommonConnectorConfig.class,
                    AbstractReadOnlyIncrementalSnapshotChangeEventSource.class).newInstance(config, this);
        }
        catch (Throwable t) {
            throw new DebeziumException("Unable to instantiate external signal class '" + externalSignalClassName + "'", t);
        }

    }

    public abstract void enqueueDataCollectionNamesToSnapshot(List<String> dataCollectionIds, long signalOffset);
}
