/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static io.debezium.data.Envelope.FieldName.OPERATION;
import static io.debezium.transforms.ExtractNewRecordStateConfigDefinition.CONFIG_FIELDS;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.ReplaceField;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Field;
import io.debezium.util.BoundedConcurrentHashMap;
import io.debezium.util.Strings;

/**
 * Debezium generates CDC (<code>Envelope</code>) records that are struct of values containing values
 * <code>before</code> and <code>after change</code>. Sink connectors usually are not able to work
 * with a complex structure so a user use this SMT to extract <code>after</code> value and send it down
 * unwrapped in <code>Envelope</code>.
 * <p>
 * The functionality is similar to <code>ExtractField</code> SMT but has a special semantics for handling
 * delete events; when delete event is emitted by database then Debezium emits two messages: a delete
 * message and a tombstone message that serves as a signal to Kafka compaction process.
 * <p>
 * The SMT by default drops the tombstone message created by Debezium and converts the delete message into
 * a tombstone message that can be dropped, too, if required.
 * <p>
 * The SMT also has the option to insert fields from the original record (e.g. 'op' or 'source.ts_ms') into the
 * unwrapped record or add them as header attributes.
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Jiri Pechanec
 */
public class ExtractNewRecordState<R extends ConnectRecord<R>> extends AbstractExtractNewRecordState<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractNewRecordState.class);

    private static final String EXCLUDE = "exclude";
    private static final int SCHEMA_CACHE_SIZE = 64;

    private static final Field DROP_FIELDS_HEADER = Field.create("drop.fields.header.name")
            .withDisplayName("Specifies a header that contains a list of field names to be removed")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Specifies the name of a header that contains a list of fields to be removed from the event value.");

    private static final Field DROP_FIELDS_FROM_KEY = Field.create("drop.fields.from.key")
            .withDisplayName("Specifies whether the fields to be dropped should also be omitted from the key")
            .withType(ConfigDef.Type.BOOLEAN)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(false)
            .withDescription("Specifies whether to apply the drop fields behavior to the event key as well as the value. "
                    + "Default behavior is to only remove fields from the event value, not the key.");

    private static final Field DROP_FIELDS_KEEP_SCHEMA_COMPATIBLE = Field.create("drop.fields.keep.schema.compatible")
            .withDisplayName("Specifies if fields are dropped, will the event's schemas be compatible")
            .withType(ConfigDef.Type.BOOLEAN)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(true)
            .withDescription("Controls the output event's schema compatibility when using the drop fields feature. "
                    + "`true`: dropped fields are removed if the schema indicates its optional leaving the schemas unchanged, "
                    + "`false`: dropped fields are removed from the key/value schemas, regardless of optionality.");

    private String dropFieldsHeaderName;
    private boolean dropFieldsFromKey;
    private boolean dropFieldsKeepSchemaCompatible;
    private BoundedConcurrentHashMap<NewRecordValueMetadata, Schema> schemaUpdateCache;

    private final Field.Set configFields = CONFIG_FIELDS.with(
            DROP_FIELDS_HEADER, DROP_FIELDS_FROM_KEY, DROP_FIELDS_KEEP_SCHEMA_COMPATIBLE);

    @Override
    public void configure(final Map<String, ?> configs) {
        super.configure(configs);

        dropFieldsHeaderName = config.getString(DROP_FIELDS_HEADER);
        dropFieldsFromKey = config.getBoolean(DROP_FIELDS_FROM_KEY);
        dropFieldsKeepSchemaCompatible = config.getBoolean(DROP_FIELDS_KEEP_SCHEMA_COMPATIBLE);
        schemaUpdateCache = new BoundedConcurrentHashMap<>(SCHEMA_CACHE_SIZE);
    }

    @Override
    public R doApply(final R record) {
        if (record.value() == null) {
            if (dropTombstones) {
                LOGGER.trace("Tombstone {} arrived and requested to be dropped", record.key());
                return null;
            }
            if (!additionalHeaders.isEmpty()) {
                Headers headersToAdd = makeHeaders(additionalHeaders, (Struct) record.value());
                headersToAdd.forEach(h -> record.headers().add(h));
            }
            return record;
        }

        if (!smtManager.isValidEnvelope(record)) {
            return record;
        }

        if (!additionalHeaders.isEmpty()) {
            Headers headersToAdd = makeHeaders(additionalHeaders, (Struct) record.value());
            headersToAdd.forEach(h -> record.headers().add(h));
        }

        R newRecord = afterDelegate.apply(record);
        if (newRecord.value() == null && beforeDelegate.apply(record).value() == null) {
            LOGGER.trace("Truncate event arrived and requested to be dropped");
            return null;
        }
        if (newRecord.value() == null) {
            if (routeByField != null) {
                Struct recordValue = requireStruct(record.value(), "Read record to set topic routing for DELETE");
                String newTopicName = recordValue.getStruct("before").getString(routeByField);
                newRecord = setTopic(newTopicName, newRecord);
            }

            if (!Strings.isNullOrBlank(dropFieldsHeaderName)) {
                newRecord = dropFields(newRecord);
            }

            // Handling delete records
            switch (handleDeletes) {
                case DROP:
                    LOGGER.trace("Delete message {} requested to be dropped", record.key());
                    return null;
                case REWRITE:
                    LOGGER.trace("Delete message {} requested to be rewritten", record.key());
                    R oldRecord = beforeDelegate.apply(record);
                    oldRecord = addFields(additionalFields, record, oldRecord);

                    return removedDelegate.apply(oldRecord);
                default:
                    return newRecord;
            }
        }
        else {
            // Add on any requested source fields from the original record to the new unwrapped record
            if (routeByField != null) {
                Struct recordValue = requireStruct(newRecord.value(), "Read record to set topic routing for CREATE / UPDATE");
                String newTopicName = recordValue.getString(routeByField);
                newRecord = setTopic(newTopicName, newRecord);
            }

            newRecord = addFields(additionalFields, record, newRecord);

            if (!Strings.isNullOrEmpty(dropFieldsHeaderName)) {
                newRecord = dropFields(newRecord);
            }

            // Handling insert and update records
            switch (handleDeletes) {
                case REWRITE:
                    LOGGER.trace("Insert/update message {} requested to be rewritten", record.key());
                    return updatedDelegate.apply(newRecord);
                default:
                    return newRecord;
            }
        }
    }

    @Override
    public Iterable<Field> validateConfigFields() {
        return configFields;
    }

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        Field.group(config, null, configFields.asArray());
        return config;
    }

    private R addFields(List<FieldReference> additionalFields, R originalRecord, R unwrappedRecord) {
        final Struct value = requireStruct(unwrappedRecord.value(), PURPOSE);
        Struct originalRecordValue = (Struct) originalRecord.value();

        Schema updatedSchema = schemaUpdateCache.computeIfAbsent(buildCacheKey(value, originalRecord),
                s -> makeUpdatedSchema(additionalFields, value.schema(), originalRecordValue));

        // Update the value with the new fields
        Struct updatedValue = new Struct(updatedSchema);
        for (org.apache.kafka.connect.data.Field field : value.schema().fields()) {
            // We use getWithoutDefault method (instead of get) to get the raw value of the field
            // Using get method may perform unwanted manipulation for the value (e.g: replacing null value with default value)
            updatedValue.put(field.name(), value.getWithoutDefault(field.name()));

        }

        for (FieldReference fieldReference : additionalFields) {
            Optional<Schema> schema = fieldReference.getSchema(originalRecordValue.schema());
            if (schema.isPresent()) {
                updatedValue = updateValue(fieldReference, updatedValue, originalRecordValue);
            }
        }

        return unwrappedRecord.newRecord(
                unwrappedRecord.topic(),
                unwrappedRecord.kafkaPartition(),
                unwrappedRecord.keySchema(),
                unwrappedRecord.key(),
                updatedSchema,
                updatedValue,
                unwrappedRecord.timestamp());
    }

    private NewRecordValueMetadata buildCacheKey(Struct value, R originalRecord) {
        // This is needed because in case using this SMT after ExtractChangedRecordState and HeaderToValue
        // If we only use the value schema as cache key, it will be calculated only on `read` record and any other event will use the value in cache.
        // But since ExtractChangedRecordState generates changed field with `update` or `delete` operation and then eventually copied to the payload with HeaderToValue SMT,
        // the schema in that case will never be updated since cached on the first `read` operation.
        // Using also the operation in the cache key will solve the problem.
        return new NewRecordValueMetadata(value.schema(), ((Struct) originalRecord.value()).getString(OPERATION));
    }

    private R dropFields(R record) {
        if (Strings.isNullOrBlank(dropFieldsHeaderName)) {
            // No drop field header was specified, nothing to be dropped.
            return record;
        }

        final Header dropFieldsHeader = getHeaderByName(record, dropFieldsHeaderName);
        if (dropFieldsHeader == null || dropFieldsHeader.value() == null) {
            // There was no header or the header had no value.
            return record;
        }

        final List<String> fieldNames = (List<String>) dropFieldsHeader.value();
        if (fieldNames.isEmpty()) {
            return record;
        }

        return dropValueFields(dropKeyFields(record, fieldNames), fieldNames);
    }

    private R dropKeyFields(R record, List<String> fieldNames) {
        if (dropFieldsFromKey && record.key() != null) {
            final List<String> keyFieldsToDrop = getFieldsToDropFromSchema(record.keySchema(), fieldNames);
            if (!keyFieldsToDrop.isEmpty()) {
                try (ReplaceField<R> delegate = new ReplaceField.Key<>()) {
                    delegate.configure(Map.of(EXCLUDE, Strings.join(",", keyFieldsToDrop)));
                    record = delegate.apply(record);
                }
            }
        }
        return record;
    }

    private R dropValueFields(R record, List<String> fieldNames) {
        final List<String> valueFieldsToDrop = getFieldsToDropFromSchema(record.valueSchema(), fieldNames);
        if (!valueFieldsToDrop.isEmpty()) {
            try (ReplaceField<R> delegate = new ReplaceField.Value<>()) {
                delegate.configure(Map.of(EXCLUDE, Strings.join(",", valueFieldsToDrop)));
                record = delegate.apply(record);
            }
        }
        return record;
    }

    private List<String> getFieldsToDropFromSchema(Schema schema, List<String> fieldNames) {
        if (!dropFieldsKeepSchemaCompatible) {
            return fieldNames;
        }

        final List<String> fieldsToDrop = new ArrayList<>();
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            if (field.schema().isOptional() && fieldNames.contains(field.name())) {
                fieldsToDrop.add(field.name());
            }
        }
        return fieldsToDrop;
    }

    private Schema makeUpdatedSchema(List<FieldReference> additionalFields, Schema schema, Struct originalRecordValue) {
        // Get fields from original schema
        SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }

        // Update the schema with the new fields
        for (FieldReference fieldReference : additionalFields) {
            Optional<Schema> fieldSchema = fieldReference.getSchema(originalRecordValue.schema());
            if (fieldSchema.isPresent()) {
                builder = updateSchema(fieldReference, builder, fieldSchema.get());
            }
        }

        return builder.build();
    }

    private SchemaBuilder updateSchema(FieldReference fieldReference, SchemaBuilder builder, Schema fieldSchema) {
        return builder.field(fieldReference.getNewField(), fieldSchema);
    }

    private Struct updateValue(FieldReference fieldReference, Struct updatedValue, Struct struct) {
        return updatedValue.put(fieldReference.getNewField(), fieldReference.getValue(struct));
    }
}
