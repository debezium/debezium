/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.data.Envelope;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.schema.DataCollectionId;

/**
 * The class responsible for processing of signals delivered to Debezium via a dedicated signaling table.
 * The processor supports a common set of signals that it can process and every connector can register its own
 * additional signals.
 * The signalling table must conform to the structure
 * <ul>
 * <li>{@code id STRING} - the unique identifier of the signal sent, usually UUID, can be used for deduplication</li>
 * <li>{@code type STRING} - the unique logical name of the code executing the signal</li>
 * <li>{@code data STRING} - the data in JSON format that are passed to the signal code
 * </ul>
 *
 * @author Jiri Pechanec
 *
 */
@NotThreadSafe
public class Signal {

    @FunctionalInterface
    public static interface Action {

        /**
         * @param signalPayload the content of the signal
         * @return true if the signal was processed
         */
        boolean arrived(Payload signalPayload) throws InterruptedException;
    }

    public static class Payload {
        public final String id;
        public final String type;
        public final Document data;
        public final OffsetContext offsetContext;
        public final Struct source;

        /**
         * @param id identifier of the signal intended for deduplication, usually ignored by the signal
         * @param type of the signal, usually ignored by the signal, should be used only when a signal code is shared for mutlple signals
         * @param data data specific for given signal instance
         * @param offsetContext offset at what the signal was sent
         * @param source source info about position at what the signal was sent
         */
        public Payload(String id, String type, Document data, OffsetContext offsetContext, Struct source) {
            super();
            this.id = id;
            this.type = type;
            this.data = data;
            this.offsetContext = offsetContext;
            this.source = source;
        }

        @Override
        public String toString() {
            return "Payload [id=" + id + ", type=" + type + ", data=" + data + ", offsetContext=" + offsetContext
                    + ", source=" + source + "]";
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(Signal.class);

    private final CommonConnectorConfig connectorConfig;
    private final String signalDataCollectionId;
    private final EventDispatcher<? extends DataCollectionId> dispatcher;

    private final Map<String, Action> signalActions = new HashMap<>();

    public Signal(CommonConnectorConfig connectorConfig, EventDispatcher<? extends DataCollectionId> eventDispatcher) {
        this.connectorConfig = connectorConfig;
        this.signalDataCollectionId = connectorConfig.getSignalingDataCollectionId();
        this.dispatcher = eventDispatcher;
        registerSignalAction(Log.NAME, new Log());
        if (connectorConfig instanceof HistorizedRelationalDatabaseConnectorConfig) {
            registerSignalAction(SchemaChanges.NAME,
                    new SchemaChanges(dispatcher, ((HistorizedRelationalDatabaseConnectorConfig) connectorConfig).useCatalogBeforeSchema()));
        }
        else {
            registerSignalAction(SchemaChanges.NAME, new SchemaChanges(dispatcher, false));
        }
    }

    Signal(CommonConnectorConfig connectorConfig) {
        this(connectorConfig, null);
    }

    public boolean isSignal(DataCollectionId dataCollectionId) {
        return signalDataCollectionId != null && signalDataCollectionId.equals(dataCollectionId.identifier());
    }

    public void registerSignalAction(String id, Action signal) {
        LOGGER.debug("Registering signal '{}' using class '{}'", id, signal.getClass().getName());
        signalActions.put(id, signal);
    }

    public boolean process(String id, String type, String data, OffsetContext offset, Struct source) throws InterruptedException {
        LOGGER.debug("Arrived signal id = '{}', type = '{}', data = '{}'", id, type, data);
        final Action action = signalActions.get(type);
        if (action == null) {
            LOGGER.warn("Signal '{}' has arrived but the type '{}' is not recognized", id, type);
            return false;
        }
        try {
            final Document jsonData = (data == null || data.isEmpty()) ? Document.create()
                    : DocumentReader.defaultReader().read(data);
            return action.arrived(new Payload(id, type, jsonData, offset, source));
        }
        catch (IOException e) {
            LOGGER.warn("Signal '{}' has arrived but the data '{}' cannot be parsed", id, data, e);
            return false;
        }
    }

    public boolean process(String id, String type, String data) throws InterruptedException {
        return process(id, type, data, null, null);
    }

    /**
     * 
     * @param value Envelope with change from signaling table
     * @param offset offset of the incoming signal
     * @return true if the signal was processed
     */
    public boolean process(Struct value, OffsetContext offset) throws InterruptedException {
        String id = null;
        String type = null;
        String data = null;
        Struct source = null;
        try {
            final Struct after = value.getStruct(Envelope.FieldName.AFTER);
            if (after == null) {
                LOGGER.warn("After part of signal '{}' is missing", value);
                return false;
            }
            if (value.schema().field(Envelope.FieldName.SOURCE) != null) {
                source = value.getStruct(Envelope.FieldName.SOURCE);
            }
            List<Field> fields = after.schema().fields();
            if (fields.size() != 3) {
                LOGGER.warn("The signal event '{}' should have 3 fields but has {}", after, fields.size());
                return false;
            }
            id = after.getString(fields.get(0).name());
            type = after.getString(fields.get(1).name());
            data = after.getString(fields.get(2).name());
        }
        catch (Exception e) {
            LOGGER.warn("Exception while preparing to process the signal '{}'", value, e);
        }
        return process(id, type, data, offset, source);
    }
}