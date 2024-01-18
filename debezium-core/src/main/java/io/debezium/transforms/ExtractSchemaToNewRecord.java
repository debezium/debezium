/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static io.debezium.config.CommonConnectorConfig.SCHEMA_NAME_ADJUSTMENT_MODE;
import static io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY;
import static io.debezium.data.Envelope.FieldName.SOURCE;
import static io.debezium.data.SchemaUtil.getSourceColumnComment;
import static io.debezium.data.SchemaUtil.getSourceColumnName;
import static io.debezium.data.SchemaUtil.getSourceColumnPrecision;
import static io.debezium.data.SchemaUtil.getSourceColumnSize;
import static io.debezium.data.SchemaUtil.getSourceColumnType;
import static io.debezium.relational.history.ConnectTableChangeSerializer.COLUMNS_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.COMMENT_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.ID_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.LENGTH_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.NAME_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.SCALE_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.TABLE_KEY;
import static io.debezium.relational.history.ConnectTableChangeSerializer.TYPE_NAME_KEY;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig.SchemaNameAdjustmentMode;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.data.SchemaUtil;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.util.BoundedConcurrentHashMap;

public class ExtractSchemaToNewRecord<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractSchemaToNewRecord.class);
    public static final String SOURCE_SCHEMA_KEY = "sourceSchema";
    private final ExtractField<R> afterDelegate = ConnectRecordUtil.extractAfterDelegate();
    private final ExtractField<R> beforeDelegate = ConnectRecordUtil.extractBeforeDelegate();
    private final BoundedConcurrentHashMap<Schema, NewRecordValueMetadata> recordValueSchemaCache = new BoundedConcurrentHashMap<>(10240);
    private final Field.Set configFields = Field.setOf();
    private SchemaNameAdjuster schemaNameAdjuster;
    private SmtManager<R> smtManager;

    @Override
    public R apply(R record) {
        // Handling tombstone record
        if (record.value() == null) {
            return record;
        }

        if (!smtManager.isValidEnvelope(record)) {
            return record;
        }

        R afterRecord = afterDelegate.apply(record);
        R beforeRecord = beforeDelegate.apply(record);
        Struct oldValue = (Struct) record.value();

        // Handling truncate record
        if (afterRecord.value() == null && beforeRecord.value() == null) {
            return record;
        }

        NewRecordValueMetadata newRecordValueMetadata = recordValueSchemaCache.computeIfAbsent(record.valueSchema(),
                key -> makeUpdatedSchema(key, oldValue, afterRecord));
        Struct newValue = new Struct(newRecordValueMetadata.schema);
        for (org.apache.kafka.connect.data.Field field : record.valueSchema().fields()) {
            Object value = oldValue.get(field);
            if (value != null) {
                newValue.put(field, value);
            }
        }
        org.apache.kafka.connect.data.Field sourceSchemaField = newRecordValueMetadata.schema.field(SOURCE_SCHEMA_KEY);
        newValue.put(sourceSchemaField, newRecordValueMetadata.metadataValue);

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                newRecordValueMetadata.schema,
                newValue,
                record.timestamp(),
                record.headers());
    }

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        Field.group(config, null, configFields.asArray());
        return config;
    }

    @Override
    public void close() {
        afterDelegate.close();
        beforeDelegate.close();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Configuration config = Configuration.from(configs);
        smtManager = new SmtManager<>(config);

        if (!config.validateAndRecord(validateConfigFields(), LOGGER::error)) {
            throw new DebeziumException("Unable to validate config.");
        }

        schemaNameAdjuster = SchemaNameAdjustmentMode.parse(config.getString(SCHEMA_NAME_ADJUSTMENT_MODE))
                .createAdjuster();
    }

    private Iterable<Field> validateConfigFields() {
        return configFields;
    }

    private NewRecordValueMetadata makeUpdatedSchema(Schema originalSchema, Struct originalValue, R afterRecord) {
        // Build new value schema with adding "sourceSchema" property
        SchemaBuilder builder = SchemaUtil.copySchema(originalSchema);
        Schema sourceSchemaBlockSchema = SchemaFactory.get().sourceSchemaBlockSchema(schemaNameAdjuster);
        builder.field(SOURCE_SCHEMA_KEY, sourceSchemaBlockSchema);
        Schema newValueSchema = builder.build();

        // Source Schema struct
        Struct sourceSchemaStruct = new Struct(sourceSchemaBlockSchema);
        Struct sourceStruct = (Struct) originalValue.get(SOURCE);
        sourceSchemaStruct.put(ID_KEY, sourceStruct.get(TABLE_NAME_KEY));
        // Table struct
        Struct table = new Struct(SchemaFactory.get().sourceSchemaBlockTableSchema(schemaNameAdjuster));
        List<Struct> columns = new ArrayList<>();
        afterRecord.valueSchema().fields().forEach(field -> {
            // Column struct
            Struct column = new Struct(SchemaFactory.get().sourceSchemaBlockColumnSchema(schemaNameAdjuster));
            Optional<String> nameOpt = getSourceColumnName(field.schema());
            Optional<String> typeNameOpt = getSourceColumnType(field.schema());
            if (nameOpt.isEmpty() || typeNameOpt.isEmpty()) {
                throw new DebeziumException("Ensure that enable configurations \"column.propagate.source.type\" " +
                        "or \"datatype.propagate.source.type\" and the value is set to \".*\"");
            }
            nameOpt.ifPresent(s -> column.put(NAME_KEY, s));
            typeNameOpt.ifPresent(s -> column.put(TYPE_NAME_KEY, s));
            getSourceColumnSize(field.schema()).ifPresent(s -> column.put(LENGTH_KEY, Integer.parseInt(s)));
            getSourceColumnPrecision(field.schema()).ifPresent(s -> column.put(SCALE_KEY, Integer.parseInt(s)));
            getSourceColumnComment(field.schema()).ifPresent(s -> column.put(COMMENT_KEY, s));
            columns.add(column);
        });
        table.put(COLUMNS_KEY, columns);
        sourceSchemaStruct.put(TABLE_KEY, table);

        return new NewRecordValueMetadata(newValueSchema, sourceSchemaStruct);
    }

    private static class NewRecordValueMetadata {
        private final Schema schema;
        private final Struct metadataValue;

        NewRecordValueMetadata(Schema schema, Struct metadataValue) {
            this.schema = schema;
            this.metadataValue = metadataValue;
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
                    Objects.equals(metadataValue, metadata.metadataValue);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schema, metadataValue);
        }

        @Override
        public String toString() {
            return "NewRecordValueMetadata{" + schema + ":" + metadataValue + "}";
        }
    }
}
