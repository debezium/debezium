/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static io.debezium.data.Envelope.FieldName.OPERATION;
import static io.debezium.data.Envelope.FieldName.SOURCE;
import static io.debezium.data.Envelope.FieldName.TIMESTAMP;
import static io.debezium.data.Envelope.FieldName.TRANSACTION;
import static io.debezium.pipeline.txmetadata.TransactionMonitor.DEBEZIUM_TRANSACTION_DATA_COLLECTION_ORDER_KEY;
import static io.debezium.pipeline.txmetadata.TransactionMonitor.DEBEZIUM_TRANSACTION_ID_KEY;
import static io.debezium.pipeline.txmetadata.TransactionMonitor.DEBEZIUM_TRANSACTION_TOTAL_ORDER_KEY;
import static io.debezium.transforms.ExtractNewRecordStateConfigDefinition.ADD_FIELDS;
import static io.debezium.transforms.ExtractNewRecordStateConfigDefinition.ADD_FIELDS_PREFIX;
import static io.debezium.transforms.ExtractNewRecordStateConfigDefinition.ADD_HEADERS;
import static io.debezium.transforms.ExtractNewRecordStateConfigDefinition.ADD_HEADERS_PREFIX;
import static io.debezium.transforms.ExtractNewRecordStateConfigDefinition.DROP_TOMBSTONES;
import static io.debezium.transforms.ExtractNewRecordStateConfigDefinition.HANDLE_DELETES;
import static io.debezium.transforms.ExtractNewRecordStateConfigDefinition.HANDLE_TOMBSTONE_DELETES;
import static io.debezium.transforms.ExtractNewRecordStateConfigDefinition.ROUTE_BY_FIELD;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.data.Envelope;
import io.debezium.transforms.ExtractNewRecordStateConfigDefinition.DeleteHandling;
import io.debezium.transforms.ExtractNewRecordStateConfigDefinition.DeleteTombstoneHandling;
import io.debezium.transforms.extractnewstate.DefaultDeleteHandlingStrategy;
import io.debezium.transforms.extractnewstate.ExtractRecordStrategy;
import io.debezium.transforms.extractnewstate.LegacyDeleteHandlingStrategy;
import io.debezium.util.Strings;

/**
 * ExtractNewRecordState and ExtractNewDocumentState that contains shared logic for transformation
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Harvey Yue
 */
public abstract class AbstractExtractNewRecordState<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractExtractNewRecordState.class);
    private static final Pattern FIELD_SEPARATOR = Pattern.compile("\\.");
    private static final Pattern NEW_FIELD_SEPARATOR = Pattern.compile(":");
    private static final String UPDATE_DESCRIPTION = "updateDescription";
    protected static final String PURPOSE = "source field insertion";

    protected Configuration config;
    protected SmtManager<R> smtManager;
    protected ExtractRecordStrategy<R> extractRecordStrategy;
    protected List<FieldReference> additionalHeaders;
    protected List<FieldReference> additionalFields;
    protected String routeByField;

    @Override
    public void configure(final Map<String, ?> configs) {
        config = Configuration.from(configs);
        smtManager = new SmtManager<>(config);

        if (!config.validateAndRecord(validateConfigFields(), LOGGER::error)) {
            throw new ConnectException("Unable to validate config.");
        }

        String addFieldsPrefix = config.getString(ADD_FIELDS_PREFIX);
        String addHeadersPrefix = config.getString(ADD_HEADERS_PREFIX);
        additionalFields = FieldReference.fromConfiguration(addFieldsPrefix, config.getString(ADD_FIELDS));
        additionalHeaders = FieldReference.fromConfiguration(addHeadersPrefix, config.getString(ADD_HEADERS));
        String routeFieldConfig = config.getString(ROUTE_BY_FIELD);
        routeByField = routeFieldConfig.isEmpty() ? null : routeFieldConfig;
        // handle deleted records
        if (!Strings.isNullOrBlank(config.getString(HANDLE_TOMBSTONE_DELETES))) {
            DeleteTombstoneHandling deleteTombstoneHandling = DeleteTombstoneHandling.parse(config.getString(HANDLE_TOMBSTONE_DELETES));
            extractRecordStrategy = new DefaultDeleteHandlingStrategy<>(deleteTombstoneHandling);
        }
        else {
            // will be removed in further release
            boolean dropTombstones = config.getBoolean(DROP_TOMBSTONES);
            DeleteHandling handleDeletes = DeleteHandling.parse(config.getString(HANDLE_DELETES));
            extractRecordStrategy = new LegacyDeleteHandlingStrategy<>(handleDeletes, dropTombstones);
            LOGGER.warn(
                    "The deleted record handling configs \"drop.tombstones\" and \"delete.handling.mode\" have been deprecated, " +
                            "please use \"delete.tombstone.handling.mode\" instead of.");
        }
    }

    @Override
    public R apply(final R record) {
        return doApply(record);
    }

    protected abstract R doApply(R record);

    protected abstract Iterable<Field> validateConfigFields();

    @Override
    public void close() {
        if (extractRecordStrategy != null) {
            extractRecordStrategy.close();
        }
    }

    /**
     * Set a new topic to connect record
     */
    protected R setTopic(String updatedTopicValue, R record) {
        return Strings.isNullOrBlank(updatedTopicValue) ? record
                : record.newRecord(
                        updatedTopicValue,
                        record.kafkaPartition(),
                        record.keySchema(),
                        record.key(),
                        record.valueSchema(),
                        record.value(),
                        record.timestamp());
    }

    /**
     * Create the Headers object which contains the headers to be added.
     */
    protected Headers makeHeaders(List<FieldReference> additionalHeaders, Struct originalRecordValue) {
        Headers headers = new ConnectHeaders();

        for (FieldReference fieldReference : additionalHeaders) {
            // add "d" operation header to tombstone events
            if (originalRecordValue == null) {
                if (OPERATION.equals(fieldReference.getField())) {
                    headers.addString(fieldReference.getNewField(), Envelope.Operation.DELETE.code());
                }
                continue;
            }
            Optional<Schema> schema = fieldReference.getSchema(originalRecordValue.schema());
            schema.ifPresent(value -> headers.add(fieldReference.getNewField(), fieldReference.getValue(originalRecordValue), value));
        }

        return headers;
    }

    /**
     * Retrieve the Header object from connect record by specified header name
     */
    protected Header getHeaderByName(R record, String headerName) {
        for (Header header : record.headers()) {
            if (header.key().equals(headerName)) {
                return header;
            }
        }
        return null;
    }

    /**
     * Represents a field that should be added to the outgoing record as a header
     * attribute or struct field.
     */
    protected static class FieldReference {

        /**
         * The struct ("source", "transaction") hosting the given field, or {@code null} for "op" and "ts_ms".
         */
        private final String struct;

        /**
         * The simple field name.
         */
        private final String field;

        /**
         * The name for the outgoing attribute/field, e.g. "__op" or "__source_ts_ms" when the prefix is "__"
         */
        private final String newField;

        private FieldReference(String prefix, String field) {
            String[] parts = NEW_FIELD_SEPARATOR.split(field);
            String[] splits = FIELD_SEPARATOR.split(parts[0]);
            this.field = splits.length == 1 ? splits[0] : splits[1];
            this.struct = (splits.length == 1) ? determineStruct(this.field) : splits[0];

            if (parts.length == 1) {
                this.newField = prefix + (splits.length == 1 ? this.field : this.struct + "_" + this.field);
            }
            else if (parts.length == 2) {
                this.newField = prefix + parts[1];
            }
            else {
                throw new IllegalArgumentException("Unexpected field name: " + field);
            }
        }

        /**
         * Determines the struct hosting the given unqualified field.
         */
        private static String determineStruct(String simpleFieldName) {

            switch (simpleFieldName) {
                case DEBEZIUM_TRANSACTION_ID_KEY:
                case DEBEZIUM_TRANSACTION_DATA_COLLECTION_ORDER_KEY:
                case DEBEZIUM_TRANSACTION_TOTAL_ORDER_KEY:
                    return TRANSACTION;
                case OPERATION:
                case TIMESTAMP:
                    return null;
                // for mongodb
                case UPDATE_DESCRIPTION:
                    return UPDATE_DESCRIPTION;
                default:
                    return SOURCE;
            }
        }

        public static List<FieldReference> fromConfiguration(String fieldPrefix, String addHeadersConfig) {
            if (Strings.isNullOrEmpty(addHeadersConfig)) {
                return Collections.emptyList();
            }
            else {
                return Arrays.stream(addHeadersConfig.split(","))
                        .map(String::trim)
                        .map(field -> new FieldReference(fieldPrefix, field))
                        .collect(Collectors.toList());
            }
        }

        protected String getField() {
            return field;
        }

        public String getNewField() {
            return this.newField;
        }

        public Object getValue(Struct originalRecordValue) {
            Struct parentStruct = struct != null ? (Struct) originalRecordValue.getWithoutDefault(struct) : originalRecordValue;

            // transaction is optional; e.g. not present during snapshotting atm.
            return parentStruct != null ? getWithoutDefault(parentStruct, originalRecordValue) : null;
        }

        private Object getWithoutDefault(Struct parentStruct, Struct originalRecordValue) {
            // In case field was added by other SMT and is in the main payload
            // object.
            return isInSchema(parentStruct.schema()) ? parentStruct.getWithoutDefault(field)
                    : originalRecordValue.getWithoutDefault(field);
        }

        public Optional<Schema> getSchema(Schema originalRecordSchema) {

            Optional<org.apache.kafka.connect.data.Field> extractedField = getField(originalRecordSchema);
            return extractedField.map(value -> SchemaUtil.copySchemaBasics(value.schema()).optional().build());

        }

        private Optional<org.apache.kafka.connect.data.Field> getField(Schema originalRecordSchema) {

            Schema parentSchema = struct != null ? originalRecordSchema.field(struct).schema() : originalRecordSchema;

            org.apache.kafka.connect.data.Field schemaField = parentSchema.field(field);

            if (schemaField == null) {
                LOGGER.debug("Field {} not found in {}. Trying in main payload", field, struct);
                if (!isInSchema(originalRecordSchema)) {
                    return Optional.empty();
                }
                schemaField = originalRecordSchema.field(field);
            }

            return Optional.of(schemaField);
        }

        private boolean isInSchema(Schema originalRecordSchema) {
            return originalRecordSchema.field(field) != null;
        }
    }

    protected static class NewRecordValueMetadata {
        private final Schema schema;
        private final String operation;

        NewRecordValueMetadata(Schema schema, String operation) {
            this.schema = schema;
            this.operation = operation;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            NewRecordValueMetadata metadata = (NewRecordValueMetadata) o;
            return Objects.equals(schema, metadata.schema) &&
                    Objects.equals(operation, metadata.operation);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schema, operation);
        }

        @Override
        public String toString() {
            return "NewRecordValueMetadata{" + schema + ":" + operation + "}";
        }
    }
}
