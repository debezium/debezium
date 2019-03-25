/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.outbox;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.transforms.outbox.EventRouterConfigDefinition.AdditionalField;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.RegexRouter;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.debezium.transforms.outbox.EventRouterConfigDefinition.parseAdditionalFieldsConfig;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * Debezium Outbox Transform Event Router
 *
 * @author Renato mefi (gh@mefi.in)
 */
public class EventRouter<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventRouter.class);

    private static final String ENVELOPE_EVENT_TYPE = "eventType";
    private static final String ENVELOPE_PAYLOAD = "payload";

    private final ExtractField<R> afterExtractor = new ExtractField.Value<>();
    private final RegexRouter<R> regexRouter = new RegexRouter<>();
    private EventRouterConfigDefinition.InvalidOperationBehavior invalidOperationBehavior;

    private String fieldEventId;
    private String fieldEventKey;
    private String fieldEventType;
    private String fieldEventTimestamp;
    private String fieldPayload;
    private String fieldPayloadId;

    private String routeByField;

    private Schema valueSchema;
    private List<AdditionalField> additionalFields;

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
        Schema eventValueSchema = afterRecord.valueSchema();

        Long timestamp = fieldEventTimestamp == null
                ? debeziumEventValue.getInt64("ts_ms")
                : eventStruct.getInt64(fieldEventTimestamp);

        Object eventId = eventStruct.get(fieldEventId);
        Object eventType = eventStruct.get(fieldEventType);
        Object payload = eventStruct.get(fieldPayload);
        Object payloadId = eventStruct.get(fieldPayloadId);

        Headers headers = r.headers();
        headers.add("id", eventId, eventValueSchema.field(fieldEventId).schema());

        if (valueSchema == null) {
            valueSchema = buildValueSchema(eventValueSchema);
        }

        Struct value = new Struct(valueSchema)
                .put(ENVELOPE_EVENT_TYPE, eventType)
                .put(ENVELOPE_PAYLOAD, payload);

        additionalFields.forEach((additionalField -> {
            switch (additionalField.getPlacement()) {
                case ENVELOPE:
                    value.put(
                            additionalField.getAlias(),
                            eventStruct.get(additionalField.getField())
                    );
                    break;
                case HEADER:
                    headers.add(
                            additionalField.getAlias(),
                            eventStruct.get(additionalField.getField()),
                            eventValueSchema.field(additionalField.getField()).schema()
                    );
                    break;
            }
        }));

        R newRecord = r.newRecord(
                eventStruct.getString(routeByField).toLowerCase(),
                null,
                Schema.STRING_SCHEMA,
                defineRecordKey(eventStruct, payloadId),
                valueSchema,
                value,
                timestamp,
                headers
        );

        return regexRouter.apply(newRecord);
    }

    private Object defineRecordKey(Struct eventStruct, Object fallbackKey) {
        Object eventKey = null;
        if (fieldEventKey != null) {
            eventKey = eventStruct.get(fieldEventKey);
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
        fieldEventTimestamp = config.getString(EventRouterConfigDefinition.FIELD_EVENT_TIMESTAMP);
        fieldPayload = config.getString(EventRouterConfigDefinition.FIELD_PAYLOAD);
        fieldPayloadId = config.getString(EventRouterConfigDefinition.FIELD_PAYLOAD_ID);

        routeByField = config.getString(EventRouterConfigDefinition.ROUTE_BY_FIELD);

        final Map<String, String> regexRouterConfig = new HashMap<>();
        regexRouterConfig.put("regex", config.getString(EventRouterConfigDefinition.ROUTE_TOPIC_REGEX));
        regexRouterConfig.put("replacement", config.getString(EventRouterConfigDefinition.ROUTE_TOPIC_REPLACEMENT));

        regexRouter.configure(regexRouterConfig);

        final Map<String, String> afterExtractorConfig = new HashMap<>();
        afterExtractorConfig.put("field", Envelope.FieldName.AFTER);

        afterExtractor.configure(afterExtractorConfig);

        additionalFields = parseAdditionalFieldsConfig(config);
    }

    private Schema buildValueSchema(Schema debeziumEventSchema) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct();

        // Add default fields
        schemaBuilder
                .field(ENVELOPE_EVENT_TYPE, debeziumEventSchema.field(fieldEventType).schema())
                .field(ENVELOPE_PAYLOAD, debeziumEventSchema.field(fieldPayload).schema());

        // Add additional fields while keeping the schema inherited from Debezium based on the table column type
        additionalFields.forEach((additionalField -> {
            if (additionalField.getPlacement() == EventRouterConfigDefinition.AdditionalFieldPlacement.ENVELOPE) {
                schemaBuilder.field(
                        additionalField.getAlias(),
                        debeziumEventSchema.field(additionalField.getField()).schema()
                );
            }
        }));

        return schemaBuilder.build();
    }
}
