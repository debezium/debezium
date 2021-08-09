/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.util.BoundedConcurrentHashMap;

/**
 * Some Debezium connectors (e.g., cassandra) generate CDC records where each value within the record is wrapped
 * in a struct that includes additional properties. This SMT unwraps the values from such CDC records.
 *
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 */
public class UnwrapStructValues<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(UnwrapStructValues.class);

    private static final int SCHEMA_CACHE_SIZE = 64;
    private static final String KEY_CONFIG = "key";
    private static final String KEY_DEFAULT = "value";

    private BoundedConcurrentHashMap<Schema, Schema> schemaUpdateCache;
    private SmtManager<R> smtManager;

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(KEY_CONFIG, ConfigDef.Type.STRING, KEY_DEFAULT, ConfigDef.Importance.MEDIUM,
                    "Key name to extract the value from the wrapped structure");

    private String key;

    protected Schema operatingValueSchema(R record) {
        return record.valueSchema();
    }

    protected Object operatingValue(R record) {
        return record.value();
    }

    protected Schema operatingKeySchema(R record) {
        return record.keySchema();
    }

    protected Object operatingKey(R record) {
        return record.key();
    }

    protected R newValueRecord(R record, Schema updatedSchema, Object updatedValue) {
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }

    protected R newKeyRecord(R record, Schema updatedSchema, Object updatedValue) {
        return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
    }

    private static final String PURPOSE = "unwrap struct values";

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        smtManager = new SmtManager<>(config);
        key = config.getString(KEY_CONFIG, KEY_DEFAULT);
        schemaUpdateCache = new BoundedConcurrentHashMap<>(SCHEMA_CACHE_SIZE);
    }

    @Override
    public R apply(R record) {
        if (record.value() == null || !smtManager.isValidEnvelope(record)) {
            return record;
        }

        if (operatingValueSchema(record) == null) {
            return applySchemaless(record);
        }
        else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = Requirements.requireMapOrNull(operatingValue(record), PURPOSE);
        final HashMap<String, Object> updatedValue = new HashMap<>(value);
        if (value != null) {
            for (Map.Entry<String, Object> item : value.entrySet()) {
                if (needsUnwrapStruct(item.getKey())) {
                    final Map<String, Object> itemValue = Requirements.requireMapOrNull(item.getValue(), PURPOSE);
                    if (itemValue != null) {
                        for (Map.Entry<String, Object> fieldEntry : itemValue.entrySet()) {
                            final Struct fieldValue = Requirements.requireStruct(fieldEntry.getValue(), PURPOSE);
                            itemValue.put(item.getKey(), fieldValue.get(key));
                        }
                    }
                    updatedValue.put(item.getKey(), itemValue);
                }
                else {
                    updatedValue.put(item.getKey(), item.getValue());
                }
            }
        }
        return newValueRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = Requirements.requireStructOrNull(operatingValue(record), PURPOSE);
        final Schema valueSchema = operatingValueSchema(record);
        final Schema updatedSchema = schemaUpdateCache.computeIfAbsent(value.schema(), s -> convertSchema(valueSchema));
        final Struct updatedValue = new Struct(updatedSchema);
        if (value != null) {
            for (Field item : value.schema().fields()) {
                if (needsUnwrapStruct(item.name())) {
                    Struct updateValue2 = new Struct(updatedSchema.schema().field(item.name()).schema());
                    final Struct itemValue = Requirements.requireStruct(value.get(item.name()), PURPOSE);
                    for (Field fieldSchema : item.schema().fields()) {
                        final Struct fieldValue = Requirements.requireStruct(itemValue.get(fieldSchema.name()), PURPOSE);
                        updateValue2.put(fieldSchema.name(), fieldValue.get(key));
                    }
                    updatedValue.put(item.name(), updateValue2);
                }
                else {
                    updatedValue.put(item.name(), value.get(item));
                }
            }
        }
        return newValueRecord(record, updatedSchema, updatedValue);
    }

    private Schema convertSchema(Schema originalSchema) {
        final SchemaBuilder builder = SchemaBuilder.struct().name(originalSchema.name());
        for (Field item : originalSchema.fields()) {
            if (needsUnwrapStruct(item.name())) {
                SchemaBuilder builder2 = SchemaBuilder.struct().name(item.schema().name());
                for (Field fieldSchema : item.schema().fields()) {
                    builder2.field(fieldSchema.name(), fieldSchema.schema().field(key).schema());
                }
                builder.field(item.name(), builder2.build());
            }
            else {
                builder.field(item.name(), item.schema());
            }
        }
        return builder.build();
    }

    private boolean needsUnwrapStruct(String name) {
        return "after".equals(name) || "before".equals(name);
    }

    @Override
    public void close() {
    }
}
