/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public class ExtractConnectField<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String FIELD_CONFIG = "field";
    private static final String TOPICS_CONFIG = "topics";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    FIELD_CONFIG,
                    ConfigDef.Type.STRING,
                    null,
                    ConfigDef.Importance.MEDIUM,
                    "Field name to be extracted ")
            .define(
                    TOPICS_CONFIG,
                    ConfigDef.Type.LIST,
                    Collections.emptyList(),
                    ConfigDef.Importance.HIGH,
                    "Topic names for which this transformation will be applied."
            );
    private static final String PURPOSE = "get after value fieldToExtract as seperate fieldToExtract in record";

    private List<String> topics;
    private String fieldToExtract;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        fieldToExtract = config.getString(FIELD_CONFIG);
        topics = config.getList(TOPICS_CONFIG);
    }

    @Override
    public R apply(R record) {
        if (!topics.isEmpty() && !topics.contains(record.topic())) {
            return record;
        }

        if (record.valueSchema() == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(final R record) {
        final Map<String, Object> value = requireMap(record.value(), PURPOSE);
        final Map<String, Object> afterValue = getMap(value, "after");
        final Map<String, Object> innerValue = afterValue != null ? afterValue : getMap(value, "before");

        if (innerValue == null) {
            throw new UnsupportedOperationException("Applying on records that do not have 'before' and 'after' fields is not defined.");
        }

        value.put(fieldToExtract, innerValue.get(fieldToExtract));

        return record;
    }

    private Map<String, Object> getMap(final Map<String, Object> value, final String after) {
        try {
            @SuppressWarnings("unchecked")
            final Map<String, Object> result = (Map<String, Object>) value.get(after);
            return result;
        } catch (ClassCastException e) {
            return null;
        }
    }

    private R applyWithSchema(final R record) {
        final Struct value = requireStruct(record.value(), PURPOSE);
        final Struct innerValue = value.getStruct("after") != null
                ? value.getStruct("after")
                : value.getStruct("before");

        if (innerValue == null) {
            throw new UnsupportedOperationException("Applying on records that do not have 'before' and 'after' fields is not defined.");
        }

        final Object extractedValue = innerValue.get(fieldToExtract);
        final Schema extractedFieldSchema = innerValue.schema().field(fieldToExtract).schema();

        final Schema newSchema = rebuildSchemaWithNewField(value, extractedFieldSchema);
        final Struct newValues = rebuildValuesWithNewField(value, extractedValue, newSchema);

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                newSchema,
                newValues,
                record.timestamp());
    }

    private Struct rebuildValuesWithNewField(Struct value, Object extractedValue, Schema newSchema) {
        Struct newValues = new Struct(newSchema);
        for (Field field : value.schema().fields()) {
            newValues.put(field.name(), value.get(field));
        }

        newValues.put(fieldToExtract, extractedValue);
        return newValues;
    }

    private Schema rebuildSchemaWithNewField(Struct value, Schema extractedFieldSchema) {
        SchemaBuilder newSchemaBuilder = SchemaBuilder.struct();
        for (Field field : value.schema().fields()) {
            newSchemaBuilder.field(field.name(), field.schema());
        }

        newSchemaBuilder.field(fieldToExtract, extractedFieldSchema);
        return newSchemaBuilder.build();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {}

}
