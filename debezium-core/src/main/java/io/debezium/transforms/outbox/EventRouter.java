/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.outbox;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * Debezium Outbox Transform Event Router
 *
 * @author Renato mefi (gh@mefi.in)
 */
public class EventRouter<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventRouter.class);

    private final ExtractField<R> afterExtractor = new ExtractField.Value<>();
    private EventRouterConfigDefinition.InvalidOperationBehavior invalidOperationBehavior;

    private String fieldEventId;
    private String fieldEventKey;
    private String fieldEventType;
    private String fieldPayload;
    private String fieldPayloadId;
    private String fieldPayloadType;

    private Schema valueSchema;

    @Override
    public R apply(R r) {
        // Ignoring tombstones
        if (r.value() == null) {
            LOGGER.info("Tombstone message {} ignored", r.key());
            return null;
        }

        Struct debeziumEventValue = requireStruct(r.value(), "Detect Debezium Operation");
        String op = debeziumEventValue.getString(Envelope.FieldName.OPERATION);

        // Skipping deletes
        if (op.equals(Envelope.Operation.DELETE.code())) {
            LOGGER.info("Delete message {} ignored", r.key());
            return null;
        }

        // Dealing with unexpected update operations
        if (op.equals(Envelope.Operation.UPDATE.code())) {
            handleUnexpectedOperation(r);
            return null;
        }

        final R afterRecord = afterExtractor.apply(r);
        Struct eventStruct = requireStruct(afterRecord.value(), "Read Outbox Event");

        Long timestamp = debeziumEventValue.getInt64("ts_ms");

        String eventId = eventStruct.getString(fieldEventId);
        String eventType = eventStruct.getString(fieldEventType);
        String payload = eventStruct.getString(fieldPayload);
        String payloadId = eventStruct.getString(fieldPayloadId);
        String payloadType = eventStruct.getString(fieldPayloadType);

        Headers headers = r.headers();
        headers.addString("id", eventId);

        Struct value = new Struct(valueSchema)
                .put("eventType", eventType)
                .put("payload", payload);

        return r.newRecord(
                payloadType.toLowerCase(),
                null,
                Schema.STRING_SCHEMA,
                defineRecordKey(eventStruct, payloadId),
                valueSchema,
                value,
                timestamp,
                headers
        );
    }

    private String defineRecordKey(Struct eventStruct, String fallbackKey) {
        String eventKey = null;
        if (fieldEventKey != null) {
            eventKey = eventStruct.getString(fieldEventKey);
        }

        return (eventKey != null) ? eventKey : fallbackKey;
    }

    private void handleUnexpectedOperation(R r) {
        switch (invalidOperationBehavior) {
            case SKIP_AND_WARN:
                LOGGER.warn("Unexpected update message received {} and ignored", r.key());
                break;
            case SKIP_AND_ERROR:
                LOGGER.error("Unexpected update message received {} and ignored", r.key());
                break;
            case FATAL:
                throw new IllegalStateException(String.format("Unexpected update message received %s, fail.", r.key()));
        }
    }

    @Override
    public ConfigDef config() {
        return EventRouterConfigDefinition.configDef();
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configMap) {
        final Configuration config = Configuration.from(configMap);

        invalidOperationBehavior = EventRouterConfigDefinition.InvalidOperationBehavior.parse(
                config.getString(EventRouterConfigDefinition.OPERATION_INVALID_BEHAVIOR)
        );

        fieldEventId = config.getString(EventRouterConfigDefinition.FIELD_EVENT_ID);
        fieldEventKey = config.getString(EventRouterConfigDefinition.FIELD_EVENT_KEY);
        fieldEventType = config.getString(EventRouterConfigDefinition.FIELD_EVENT_TYPE);
        fieldPayload = config.getString(EventRouterConfigDefinition.FIELD_PAYLOAD);
        fieldPayloadId = config.getString(EventRouterConfigDefinition.FIELD_PAYLOAD_ID);
        fieldPayloadType = config.getString(EventRouterConfigDefinition.FIELD_PAYLOAD_TYPE);

        final Map<String, String> afterExtractorConfig = new HashMap<>();
        afterExtractorConfig.put("field", Envelope.FieldName.AFTER);

        afterExtractor.configure(afterExtractorConfig);

        valueSchema = SchemaBuilder.struct()
                .field("eventType", Schema.STRING_SCHEMA)
                .field("payload", Schema.STRING_SCHEMA)
                .build();
    }
}
