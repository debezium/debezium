/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal;

import java.util.List;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.common.RelationalBaseSourceConnector;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.pipeline.source.snapshot.incremental.AbstractReadOnlyIncrementalSnapshotChangeEventSource;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Threads;

/**
 * The class responsible for processing of signals delivered to Debezium via a dedicated Kafka topic.
 * The signal message must have the following structure:
 * <ul>
 * <li>{@code id STRING} - the unique identifier of the signal sent, usually UUID, can be used for deduplication</li>
 * <li>{@code type STRING} - the unique logical name of the code executing the signal</li>
 * <li>{@code data STRING} - the data in JSON format that are passed to the signal code
 * </ul>
 */
public abstract class AbstractExternalSignalThread<T extends DataCollectionId> implements ExternalSignalThread {

    public static final String CONFIGURATION_FIELD_PREFIX_STRING = "signal.";
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractExternalSignalThread.class);
    protected final ExecutorService signalListenerExecutor;
    protected final String connectorName;
    protected final AbstractReadOnlyIncrementalSnapshotChangeEventSource<T> eventSource;
    protected final DocumentWriter writer = DocumentWriter.defaultWriter();
    protected final DocumentReader reader = DocumentReader.defaultReader();

    public AbstractExternalSignalThread(CommonConnectorConfig connectorConfig,
                                        AbstractReadOnlyIncrementalSnapshotChangeEventSource<T> eventSource) {
        connectorName = connectorConfig.getLogicalName();
        try {
            Class<RelationalBaseSourceConnector> connectorClass = connectorConfig.getConnectorClass();
            signalListenerExecutor = Threads.newSingleThreadExecutor(connectorClass, connectorName, signalName(), true);
        }
        catch (ClassNotFoundException e) {
            throw new DebeziumException("Unable to load connector class", e);
        }
        this.eventSource = eventSource;
    }

    public void start() {
        signalListenerExecutor.submit(this::monitorSignals);
    }

    protected String signalName() {
        return this.getClass().getName() + "-signal";
    }

    protected abstract void monitorSignals();

    protected void processSignal(Document jsonData, long signalOffset) {
        String type = jsonData.getString("type");
        Document data = jsonData.getDocument("data");
        if (ExecuteSnapshot.NAME.equals(type)) {
            executeSnapshot(data, signalOffset);
        }
        else {
            LOGGER.warn("Unknown signal type {}", type);
        }
    }

    protected void executeSnapshot(Document data, long signalOffset) {
        final List<String> dataCollections = ExecuteSnapshot.getDataCollections(data);
        if (dataCollections != null) {
            ExecuteSnapshot.SnapshotType snapshotType = ExecuteSnapshot.getSnapshotType(data);
            LOGGER.info("Requested '{}' snapshot of data collections '{}'", snapshotType, dataCollections);
            if (snapshotType == ExecuteSnapshot.SnapshotType.INCREMENTAL) {
                eventSource.enqueueDataCollectionNamesToSnapshot(dataCollections, signalOffset);
            }
        }
    }

}
