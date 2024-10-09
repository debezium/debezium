/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.outbox;

import static io.debezium.transforms.outbox.EventRouterConfigDefinition.parseAdditionalFieldsConfig;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.RegexRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.CommonConnectorConfig.FieldNameAdjustmentMode;
import io.debezium.config.Configuration;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import io.debezium.schema.FieldNameSelector;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Timestamp;
import io.debezium.transforms.ConnectRecordUtil;
import io.debezium.transforms.SmtManager;
import io.debezium.transforms.outbox.EventRouterConfigDefinition.AdditionalField;
import io.debezium.transforms.outbox.EventRouterConfigDefinition.AdditionalFieldPlacement;
import io.debezium.transforms.outbox.EventRouterConfigDefinition.InvalidOperationBehavior;
import io.debezium.transforms.outbox.EventRouterConfigDefinition.JsonPayloadNullFieldBehavior;
import io.debezium.transforms.tracing.ActivateTracingSpan;
import io.debezium.util.BoundedConcurrentHashMap;

/**
 * A delegate class having common logic between Outbox Event Routers for SQL DBs and MongoDB
 *
 * @author Sungho Hwang
 */
public class EventRouterDelegate<R extends ConnectRecord<R>> {

    @FunctionalInterface
    public interface RecordConverter<R> {
        R convert(R record);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(EventRouterDelegate.class);

    private static final String ENVELOPE_PAYLOAD = "payload";

    private ExtractField<R> afterExtractor;
    private final RegexRouter<R> regexRouter = new RegexRouter<>();
    private InvalidOperationBehavior invalidOperationBehavior;
    private final ActivateTracingSpan<R> tracingSmt = new ActivateTracingSpan<>();

    private final Map<String, EventRouterConfigurationProvider> configurationProviders = new HashMap<>();
    private final DefaultConfigurationProvider defaultConfigurationProvider = new DefaultConfigurationProvider();

    private String fieldSchemaVersion;
    private boolean routeTombstoneOnEmptyPayload;

    private List<AdditionalField> additionalFields;
    private boolean additionalFieldsErrorOnMissing;

    private final Map<Integer, Schema> versionedValueSchema = new HashMap<>();
    private BoundedConcurrentHashMap<Schema, Schema> payloadSchemaCache;

    private boolean onlyHeadersInOutputMessage = false;

    private boolean expandJsonPayload;
    private JsonSchemaData jsonSchemaData;

    private ObjectMapper objectMapper;

    private SmtManager<R> smtManager;

    public R apply(R r, RecordConverter<R> recordConverter) {
        // Ignoring tombstones
        if (r.value() == null) {
            LOGGER.debug("Tombstone message ignored. Message key: \"{}\"", r.key());
            return null;
        }

        // Ignoring messages which do not adhere to the CDC Envelope, for instance:
        // Heartbeat and Schema Change messages
        if (!smtManager.isValidEnvelope(r)) {
            return r;
        }

        Struct debeziumEventValue = requireStruct(r.value(), "Detect Debezium Operation");
        String op = debeziumEventValue.getString(Envelope.FieldName.OPERATION);

        // Skipping deletes
        if (op.equals(Envelope.Operation.DELETE.code())) {
            LOGGER.debug("Delete message {} ignored", r.key());
            return null;
        }

        // Dealing with unexpected update operations
        if (op.equals(Envelope.Operation.UPDATE.code())) {
            handleUnexpectedOperation(r);
            return null;
        }

        r = recordConverter.convert(r);

        if (ActivateTracingSpan.isOpenTelemetryAvailable()) {
            tracingSmt.apply(r);
        }

        final R afterRecord = afterExtractor.apply(r);
        Struct eventStruct = requireStruct(afterRecord.value(), "Read Outbox Event");
        Schema eventValueSchema = afterRecord.valueSchema();

        // Get the connector-specific or default configuration provider based on the record
        Struct record = requireStruct(r.value(), "Outbox converter");
        final EventRouterConfigurationProvider configProvider = lookupConfigurationProvider(record);

        // Get the configuration values needed from the provider
        final String fieldEventId = configProvider.getFieldEventId();
        final String fieldEventKey = configProvider.getFieldEventKey();
        final String fieldPayload = configProvider.getFieldPayload();
        final String fieldEventTimestamp = configProvider.getFieldEventTimestamp();
        final String routeByField = configProvider.getRouteByField();

        final Field payloadField = eventValueSchema.field(fieldPayload);
        if (payloadField == null) {
            throw new ConnectException(String.format("Unable to find payload field %s in event", fieldPayload));
        }
        Schema payloadSchema = payloadField.schema();

        Long timestamp = getEventTimestampMs(fieldEventTimestamp, debeziumEventValue, eventStruct);
        Object eventId = eventStruct.get(fieldEventId);
        Object payload = eventStruct.get(fieldPayload);

        final Field eventIdField = eventValueSchema.field(fieldEventId);
        if (eventIdField == null) {
            throw new ConnectException(String.format("Unable to find event-id field %s in event", fieldEventId));
        }

        Headers headers = r.headers();
        headers.add("id", eventId, eventIdField.schema());

        // Check to expand JSON string into real JSON.
        if (expandJsonPayload) {
            if (!(payload instanceof String)) {
                LOGGER.warn("Expand JSON payload is turned on but payload is not a string in {}", r.key());
            }
            else {
                final String payloadString = (String) payload;

                try {
                    // Parse and get Jackson JsonNode.
                    final JsonNode jsonPayload = parseJsonPayload(payloadString);
                    // Build a new Schema and new payload Struct that replace existing ones.
                    payloadSchema = jsonSchemaData.toConnectSchema(fieldPayload, jsonPayload);
                    payload = jsonSchemaData.toConnectData(jsonPayload, payloadSchema);
                }
                catch (Exception e) {
                    LOGGER.warn("JSON expansion failed", e);
                }
            }
        }

        final Schema structValueSchema = onlyHeadersInOutputMessage ? null
                : (fieldSchemaVersion == null)
                        ? getValueSchema(payloadSchema, eventValueSchema, eventStruct.getString(routeByField))
                        : getValueSchema(payloadSchema, eventValueSchema, eventStruct.getInt32(fieldSchemaVersion), eventStruct.getString(routeByField));

        final Struct structValue = onlyHeadersInOutputMessage ? null : new Struct(structValueSchema).put(ENVELOPE_PAYLOAD, payload);

        AtomicReference<Integer> partition = new AtomicReference<>();

        additionalFields.forEach((additionalField -> {
            if (!additionalFieldsErrorOnMissing && eventStruct.schema().field(additionalField.getField()) == null) {
                return;
            }
            switch (additionalField.getPlacement()) {
                case ENVELOPE:
                    structValue.put(
                            additionalField.getAlias(),
                            eventStruct.get(additionalField.getField()));
                    break;
                case HEADER:
                    headers.add(
                            additionalField.getAlias(),
                            eventStruct.get(additionalField.getField()),
                            eventValueSchema.field(additionalField.getField()).schema());
                    break;
                case PARTITION:
                    partition.set(eventStruct.getInt32(additionalField.getField()));
            }
        }));

        boolean isDeleteEvent = payload == null || payload.toString().trim().isEmpty();

        Object updatedValue;
        Schema updatedSchema;

        if (isDeleteEvent && routeTombstoneOnEmptyPayload) {
            updatedValue = null;
            updatedSchema = null;
        }
        else if (onlyHeadersInOutputMessage) {
            updatedValue = payload;
            updatedSchema = payloadSchema;
        }
        else {
            updatedValue = structValue;
            updatedSchema = structValueSchema;
        }

        Object recordKey = defineRecordKey(fieldEventKey, eventStruct);

        R newRecord = r.newRecord(
                eventStruct.getString(routeByField),
                partition.get(),
                defineRecordKeySchema(fieldEventKey, eventValueSchema),
                recordKey,
                updatedSchema,
                updatedValue,
                timestamp,
                headers);

        LOGGER.debug("Message emitted with event id: \"{}\", event key: \"{}\"", eventId, recordKey);

        return regexRouter.apply(newRecord);
    }

    /**
     * Returns the Kafka record timestamp for the outgoing record.
     * Either obtained from the configured field or the timestamp when Debezium processed the event.
     */
    private Long getEventTimestampMs(String fieldEventTimestamp, Struct debeziumEventValue, Struct eventStruct) {
        if (fieldEventTimestamp == null) {
            return debeziumEventValue.getInt64("ts_ms");
        }

        Field timestampField = eventStruct.schema().field(fieldEventTimestamp);
        if (timestampField == null) {
            throw new ConnectException(String.format("Unable to find timestamp field %s in event", fieldEventTimestamp));
        }

        Long timestamp = eventStruct.getInt64(fieldEventTimestamp);
        if (timestamp == null) {
            return debeziumEventValue.getInt64("ts_ms");
        }

        String schemaName = timestampField.schema().name();

        if (schemaName == null) {
            throw new ConnectException(String.format("Unsupported field type %s (without logical schema name) for event timestamp", timestampField.schema().type()));
        }

        // not going through Instant here for the sake of performance
        switch (schemaName) {
            case Timestamp.SCHEMA_NAME:
                return timestamp;
            case MicroTimestamp.SCHEMA_NAME:
                return timestamp / 1_000;
            case NanoTimestamp.SCHEMA_NAME:
                return timestamp / 1_000_000;
            default:
                throw new ConnectException(String.format("Unsupported field type %s for event timestamp", schemaName));
        }
    }

    private Schema defineRecordKeySchema(String fieldEventKey, Schema eventStruct) {
        Field eventKeySchema = null;
        if (fieldEventKey != null) {
            eventKeySchema = eventStruct.field(fieldEventKey);
        }

        if (eventKeySchema != null) {
            return eventKeySchema.schema();
        }

        return Schema.STRING_SCHEMA;
    }

    private Object defineRecordKey(String fieldEventKey, Struct eventStruct) {
        return (fieldEventKey != null) ? eventStruct.get(fieldEventKey) : null;
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

    private JsonNode parseJsonPayload(String jsonString) throws Exception {
        if (jsonString.startsWith("{") || jsonString.startsWith("[")) {
            return objectMapper.readTree(jsonString);
        }
        throw new Exception("Unable to parse payload starting with '" + jsonString.charAt(0) + "'");
    }

    public ConfigDef config() {
        return EventRouterConfigDefinition.configDef();
    }

    public void close() {
        if (ActivateTracingSpan.isOpenTelemetryAvailable()) {
            tracingSmt.close();
        }
    }

    public void configure(Map<String, ?> configMap) {
        LOGGER.debug("[OTEL] OpenTelemetry.isOpenTelemetryAvailable {}", ActivateTracingSpan.isOpenTelemetryAvailable());
        if (ActivateTracingSpan.isOpenTelemetryAvailable()) {
            tracingSmt.configure(configMap);
            if (!configMap.containsKey(ActivateTracingSpan.TRACING_CONTEXT_FIELD_REQUIRED.name())) {
                tracingSmt.setRequireContextField(true);
            }
        }
        final Configuration config = Configuration.from(configMap);
        smtManager = new SmtManager<>(config);

        io.debezium.config.Field.Set allFields = io.debezium.config.Field.setOf(EventRouterConfigDefinition.CONFIG_FIELDS);
        smtManager.validate(config, allFields);

        invalidOperationBehavior = InvalidOperationBehavior.parse(
                config.getString(EventRouterConfigDefinition.OPERATION_INVALID_BEHAVIOR));

        JsonPayloadNullFieldBehavior jsonPayloadNullFieldBehavior = JsonPayloadNullFieldBehavior.parse(
                config.getString(EventRouterConfigDefinition.TABLE_JSON_PAYLOAD_NULL_BEHAVIOR));
        expandJsonPayload = config.getBoolean(EventRouterConfigDefinition.EXPAND_JSON_PAYLOAD);
        if (expandJsonPayload) {
            objectMapper = new ObjectMapper();
            FieldNameAdjustmentMode fieldNameAdjustmentMode = FieldNameAdjustmentMode.parse(
                    config.getString(CommonConnectorConfig.FIELD_NAME_ADJUSTMENT_MODE));
            jsonSchemaData = new JsonSchemaData(jsonPayloadNullFieldBehavior,
                    FieldNameSelector.defaultNonRelationalSelector(fieldNameAdjustmentMode.createAdjuster()));
        }

        // Configure the default configuration provider
        defaultConfigurationProvider.configure(configMap);

        // Allow each connector to load and supply its EventRouter configuration if applicable
        for (EventRouterConfigurationProvider provider : ServiceLoader.load(EventRouterConfigurationProvider.class)) {
            configurationProviders.put(provider.getName(), provider);
            provider.configure(configMap);
        }

        fieldSchemaVersion = config.getString(EventRouterConfigDefinition.FIELD_SCHEMA_VERSION);
        routeTombstoneOnEmptyPayload = config.getBoolean(EventRouterConfigDefinition.ROUTE_TOMBSTONE_ON_EMPTY_PAYLOAD);

        final Map<String, String> regexRouterConfig = new HashMap<>();
        regexRouterConfig.put("regex", config.getString(EventRouterConfigDefinition.ROUTE_TOPIC_REGEX));
        regexRouterConfig.put("replacement", config.getString(EventRouterConfigDefinition.ROUTE_TOPIC_REPLACEMENT));

        regexRouter.configure(regexRouterConfig);

        afterExtractor = ConnectRecordUtil.extractAfterDelegate();

        additionalFields = parseAdditionalFieldsConfig(config);
        additionalFieldsErrorOnMissing = config.getBoolean(EventRouterConfigDefinition.FIELDS_ADDITIONAL_ERROR_ON_MISSING);

        onlyHeadersInOutputMessage = additionalFields.stream().noneMatch(field -> field.getPlacement() == AdditionalFieldPlacement.ENVELOPE);

        payloadSchemaCache = new BoundedConcurrentHashMap<>(10000, 10, BoundedConcurrentHashMap.Eviction.LRU);
    }

    private Schema getValueSchema(Schema payloadSchema, Schema debeziumEventSchema, String routedTopic) {
        Schema valueSchema = payloadSchemaCache.get(payloadSchema);
        if (valueSchema == null) {
            valueSchema = getSchemaBuilder(payloadSchema, debeziumEventSchema, routedTopic).build();
            payloadSchemaCache.put(payloadSchema, valueSchema);
        }

        return valueSchema;
    }

    private Schema getValueSchema(Schema payloadSchema, Schema debeziumEventSchema, Integer version, String routedTopic) {
        if (!versionedValueSchema.containsKey(version)) {
            final Schema schema = getSchemaBuilder(payloadSchema, debeziumEventSchema, routedTopic)
                    .version(version)
                    .build();
            versionedValueSchema.put(version, schema);
        }

        return versionedValueSchema.get(version);
    }

    private SchemaBuilder getSchemaBuilder(Schema payloadSchema, Schema debeziumEventSchema, String routedTopic) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(getSchemaName(debeziumEventSchema, routedTopic));

        // Add payload field
        schemaBuilder.field(ENVELOPE_PAYLOAD, payloadSchema);

        // Add additional fields while keeping the schema inherited from Debezium based on the table column type
        additionalFields.forEach((additionalField -> {
            if (additionalField.getPlacement() == AdditionalFieldPlacement.ENVELOPE) {
                schemaBuilder.field(
                        additionalField.getAlias(),
                        debeziumEventSchema.field(additionalField.getField()).schema());
            }
        }));

        return schemaBuilder;
    }

    private String getSchemaName(Schema debeziumEventSchema, String routedTopic) {
        final String schemaName;
        final String originalSchemaName = debeziumEventSchema.name();
        if (originalSchemaName != null) {
            final int lastDot = originalSchemaName.lastIndexOf('.');
            if (lastDot != -1) {
                schemaName = originalSchemaName.substring(0, lastDot + 1) + routedTopic + "." + originalSchemaName.substring(lastDot + 1);
            }
            else {
                schemaName = routedTopic + "." + originalSchemaName;
            }
        }
        else {
            schemaName = routedTopic;
        }
        return schemaName;
    }

    /**
     * Lookup the configuration provider for the source connector or use the default if not found.
     */
    private EventRouterConfigurationProvider lookupConfigurationProvider(Struct record) {
        if (!configurationProviders.isEmpty()) {
            final Struct source = record.getStruct(Envelope.FieldName.SOURCE);
            final String connectorType = source.getString(AbstractSourceInfo.DEBEZIUM_CONNECTOR_KEY);
            final EventRouterConfigurationProvider provider = configurationProviders.get(connectorType);
            if (provider != null) {
                return provider;
            }
        }
        return defaultConfigurationProvider;
    }

    private static class DefaultConfigurationProvider implements EventRouterConfigurationProvider {

        private String fieldEventId;
        private String fieldEventKey;
        private String fieldEventTimestamp;
        private String fieldPayload;
        private String routeByField;

        @Override
        public String getName() {
            return "default";
        }

        @Override
        public void configure(Map<String, ?> configMap) {
            Configuration config = Configuration.from(configMap);
            this.fieldEventId = config.getString(EventRouterConfigDefinition.FIELD_EVENT_ID);
            this.fieldEventKey = config.getString(EventRouterConfigDefinition.FIELD_EVENT_KEY);
            this.fieldEventTimestamp = config.getString(EventRouterConfigDefinition.FIELD_EVENT_TIMESTAMP);
            this.fieldPayload = config.getString(EventRouterConfigDefinition.FIELD_PAYLOAD);
            this.routeByField = config.getString(EventRouterConfigDefinition.ROUTE_BY_FIELD);
        }

        @Override
        public String getFieldEventId() {
            return fieldEventId;
        }

        @Override
        public String getFieldEventKey() {
            return fieldEventKey;
        }

        @Override
        public String getFieldEventTimestamp() {
            return fieldEventTimestamp;
        }

        @Override
        public String getFieldPayload() {
            return fieldPayload;
        }

        @Override
        public String getRouteByField() {
            return routeByField;
        }
    }
}
