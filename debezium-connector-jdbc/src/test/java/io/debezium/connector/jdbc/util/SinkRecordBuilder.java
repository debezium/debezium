/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.util;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.sink.SinkRecord;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.jdbc.JdbcKafkaSinkRecord;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.dialect.DatabaseDialectResolver;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.SerializerType;
import io.debezium.data.Envelope;
import io.debezium.util.Strings;

/**
 * Helper class for easily building {@link SinkRecord} instances using a builder pattern.
 *
 * @author Chris Cranford
 */
public class SinkRecordBuilder {

    private SinkRecordBuilder() {
    }

    public static SinkRecordTypeBuilder create(JdbcSinkConnectorConfig config) {
        return new SinkRecordTypeBuilder(Type.CREATE, config);
    }

    public static SinkRecordTypeBuilder update(JdbcSinkConnectorConfig config) {
        return new SinkRecordTypeBuilder(Type.UPDATE, config);
    }

    public static SinkRecordTypeBuilder delete(JdbcSinkConnectorConfig config) {
        return new SinkRecordTypeBuilder(Type.DELETE, config);
    }

    public static SinkRecordTypeBuilder tombstone(JdbcSinkConnectorConfig config) {
        return new SinkRecordTypeBuilder(Type.TOMBSTONE, config);
    }

    public static SinkRecordTypeBuilder truncate(JdbcSinkConnectorConfig config) {
        return new SinkRecordTypeBuilder(Type.TRUNCATE, config);
    }

    public static SinkRecordTypeBuilder cloudEvent(JdbcSinkConnectorConfig config) {
        return new SinkRecordTypeBuilder(Type.CLOUD_EVENT, config);
    }

    public static class SinkRecordTypeBuilder {
        private final Type type;

        private boolean flat;
        private String topicName;
        private String name;
        private Schema keySchema;
        private Schema recordSchema;
        private Schema sourceSchema;
        private int partition;
        private int offset;
        private SinkRecord baseRecord;
        private SerializerType cloudEventsSerializerType;
        private String cloudEventsSchemaName = null;
        private String cloudEventsSchemaNamePattern = ".*" + Pattern.quote(CloudEventsMaker.CLOUDEVENTS_SCHEMA_SUFFIX) + "$";
        private final JdbcSinkConnectorConfig config;
        private final Map<String, Object> keyValues = new HashMap<>();
        private final Map<String, Object> beforeValues = new HashMap<>();
        private final Map<String, Object> afterValues = new HashMap<>();
        private final Map<String, Object> sourceValues = new HashMap<>();

        private SinkRecordTypeBuilder(Type type, JdbcSinkConnectorConfig config) {
            this.type = type;
            this.config = config;
        }

        public SinkRecordTypeBuilder flat(boolean flat) {
            this.flat = flat;
            return this;
        }

        public SinkRecordTypeBuilder topic(String topicName) {
            this.topicName = topicName;
            return this;
        }

        public SinkRecordTypeBuilder name(String name) {
            this.name = name;
            return this;
        }

        public SinkRecordTypeBuilder keySchema(Schema keySchema) {
            this.keySchema = keySchema;
            return this;
        }

        public SinkRecordTypeBuilder key(String fieldName, Object value) {
            keyValues.put(fieldName, value);
            return this;
        }

        public SinkRecordTypeBuilder recordSchema(Schema recordSchema) {
            this.recordSchema = recordSchema;
            return this;
        }

        public SinkRecordTypeBuilder before(String fieldName, Object value) {
            beforeValues.put(fieldName, value);
            return this;
        }

        public SinkRecordTypeBuilder after(String fieldName, Object value) {
            afterValues.put(fieldName, value);
            return this;
        }

        public SinkRecordTypeBuilder sourceSchema(Schema sourceSchema) {
            this.sourceSchema = sourceSchema;
            return this;
        }

        public SinkRecordTypeBuilder source(String fieldName, Object value) {
            sourceValues.put(fieldName, value);
            return this;
        }

        public SinkRecordTypeBuilder partition(int partition) {
            this.partition = partition;
            return this;
        }

        public SinkRecordTypeBuilder offset(int offset) {
            this.offset = offset;
            return this;
        }

        public SinkRecordTypeBuilder baseRecord(SinkRecord baseRecord) {
            this.baseRecord = baseRecord;
            return this;
        }

        public SinkRecordTypeBuilder cloudEventsSerializerType(SerializerType serializerType) {
            this.cloudEventsSerializerType = serializerType;
            return this;
        }

        public SinkRecordTypeBuilder cloudEventsSchemaName(String cloudEventsSchemaName) {
            if (null == cloudEventsSchemaName) {
                return this;
            }
            this.cloudEventsSchemaName = cloudEventsSchemaName;
            cloudEventsSchemaNamePattern = ".*" + Pattern.quote(cloudEventsSchemaName) + ".*";
            return this;
        }

        public JdbcKafkaSinkRecord build() {
            return switch (type) {
                case CREATE -> buildCreateSinkRecord();
                case UPDATE -> buildUpdateSinkRecord();
                case DELETE -> buildDeleteSinkRecord();
                case TOMBSTONE -> buildTombstoneSinkRecord();
                case TRUNCATE -> buildTruncateSinkRecord();
                case CLOUD_EVENT -> buildCloudEventRecord();
            };
        }

        private JdbcKafkaSinkRecord buildCreateSinkRecord() {
            Objects.requireNonNull(recordSchema, "A record schema must be provided.");
            Objects.requireNonNull(sourceSchema, "A source schema must be provided.");

            final Struct key = populateStructForKey();
            final Struct after = populateStructFromMap(new Struct(recordSchema), afterValues);
            final Struct source = populateStructFromMap(new Struct(sourceSchema), sourceValues);

            var sessionFactory = config.getHibernateConfiguration().buildSessionFactory();
            var databaseDialect = DatabaseDialectResolver.resolve(config, sessionFactory);

            if (!flat) {
                final Envelope envelope = createEnvelope();
                final Struct payload = envelope.create(after, source, Instant.now());

                return new JdbcKafkaSinkRecord(new SinkRecord(topicName, partition, keySchema, key, envelope.schema(), payload, offset),
                        config.getPrimaryKeyMode(),
                        config.getPrimaryKeyFields(),
                        config.getFieldFilter(),
                        cloudEventsSchemaNamePattern,
                        databaseDialect);
            }
            else {
                return new JdbcKafkaSinkRecord(new SinkRecord(topicName, partition, keySchema, key, recordSchema, after, offset),
                        config.getPrimaryKeyMode(),
                        config.getPrimaryKeyFields(),
                        config.getFieldFilter(),
                        cloudEventsSchemaNamePattern,
                        databaseDialect);
            }
        }

        private JdbcKafkaSinkRecord buildUpdateSinkRecord() {
            Objects.requireNonNull(recordSchema, "A record schema must be provided.");
            Objects.requireNonNull(sourceSchema, "A source schema must be provided.");

            final Struct key = populateStructForKey();
            final Struct after = populateStructFromMap(new Struct(recordSchema), afterValues);

            var sessionFactory = config.getHibernateConfiguration().buildSessionFactory();
            var databaseDialect = DatabaseDialectResolver.resolve(config, sessionFactory);

            if (!flat) {
                final Struct before = populateStructFromMap(new Struct(recordSchema), beforeValues);
                final Struct source = populateStructFromMap(new Struct(sourceSchema), sourceValues);
                final Envelope envelope = createEnvelope();
                final Struct payload = envelope.update(before, after, source, Instant.now());

                return new JdbcKafkaSinkRecord(new SinkRecord(topicName, partition, keySchema, key, envelope.schema(), payload, offset),
                        config.getPrimaryKeyMode(),
                        config.getPrimaryKeyFields(),
                        config.getFieldFilter(),
                        cloudEventsSchemaNamePattern,
                        databaseDialect);
            }
            else {
                return new JdbcKafkaSinkRecord(new SinkRecord(topicName, partition, keySchema, key, recordSchema, after, offset),
                        config.getPrimaryKeyMode(),
                        config.getPrimaryKeyFields(),
                        config.getFieldFilter(),
                        cloudEventsSchemaNamePattern,
                        databaseDialect);
            }
        }

        private JdbcKafkaSinkRecord buildDeleteSinkRecord() {
            Objects.requireNonNull(recordSchema, "A record schema must be provided.");
            Objects.requireNonNull(sourceSchema, "A source schema must be provided.");

            final Struct key = populateStructForKey();

            var sessionFactory = config.getHibernateConfiguration().buildSessionFactory();
            var databaseDialect = DatabaseDialectResolver.resolve(config, sessionFactory);

            if (!flat) {
                final Struct before = populateStructFromMap(new Struct(recordSchema), beforeValues);
                final Struct source = populateStructFromMap(new Struct(sourceSchema), sourceValues);
                final Envelope envelope = createEnvelope();
                final Struct payload = envelope.delete(before, source, Instant.now());

                return new JdbcKafkaSinkRecord(new SinkRecord(topicName, partition, keySchema, key, envelope.schema(), payload, offset),
                        config.getPrimaryKeyMode(),
                        config.getPrimaryKeyFields(),
                        config.getFieldFilter(),
                        cloudEventsSchemaNamePattern,
                        databaseDialect);
            }
            else {
                return new JdbcKafkaSinkRecord(new SinkRecord(topicName, partition, keySchema, key, recordSchema, null, offset),
                        config.getPrimaryKeyMode(),
                        config.getPrimaryKeyFields(),
                        config.getFieldFilter(),
                        cloudEventsSchemaNamePattern,
                        databaseDialect);
            }
        }

        private JdbcKafkaSinkRecord buildTombstoneSinkRecord() {
            final Struct key = populateStructForKey();

            var sessionFactory = config.getHibernateConfiguration().buildSessionFactory();
            var databaseDialect = DatabaseDialectResolver.resolve(config, sessionFactory);

            return new JdbcKafkaSinkRecord(new SinkRecord(topicName, partition, keySchema, key, null, null, offset),
                    config.getPrimaryKeyMode(),
                    config.getPrimaryKeyFields(),
                    config.getFieldFilter(),
                    cloudEventsSchemaNamePattern,
                    databaseDialect);
        }

        private JdbcKafkaSinkRecord buildTruncateSinkRecord() {
            var sessionFactory = config.getHibernateConfiguration().buildSessionFactory();
            var databaseDialect = DatabaseDialectResolver.resolve(config, sessionFactory);

            if (!flat) {
                final Struct source = populateStructFromMap(new Struct(sourceSchema), sourceValues);
                final Envelope envelope = createEnvelope();
                final Struct payload = envelope.truncate(source, Instant.now());
                return new JdbcKafkaSinkRecord(new SinkRecord(topicName, partition, null, null, envelope.schema(), payload, offset),
                        config.getPrimaryKeyMode(),
                        config.getPrimaryKeyFields(),
                        config.getFieldFilter(),
                        cloudEventsSchemaNamePattern,
                        databaseDialect);
            }
            else {
                return null;
            }
        }

        private JdbcKafkaSinkRecord buildCloudEventRecord() {
            final String schemaName = (cloudEventsSchemaName != null ? cloudEventsSchemaName : "test.test") + "." + CloudEventsMaker.CLOUDEVENTS_SCHEMA_SUFFIX;
            final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
                    .name(schemaName)
                    .field(CloudEventsMaker.FieldName.ID, Schema.STRING_SCHEMA)
                    .field(CloudEventsMaker.FieldName.SOURCE, Schema.STRING_SCHEMA)
                    .field(CloudEventsMaker.FieldName.SPECVERSION, Schema.STRING_SCHEMA)
                    .field(CloudEventsMaker.FieldName.TYPE, Schema.STRING_SCHEMA)
                    .field(CloudEventsMaker.FieldName.TIME, Schema.STRING_SCHEMA)
                    .field(CloudEventsMaker.FieldName.DATACONTENTTYPE, Schema.STRING_SCHEMA)
                    .field(CloudEventsMaker.FieldName.DATA, baseRecord.valueSchema());

            Schema ceSchema = schemaBuilder.build();

            Struct ceValueStruct = new Struct(ceSchema);
            ceValueStruct.put(CloudEventsMaker.FieldName.ID, Uuid.randomUuid().toString());
            ceValueStruct.put(CloudEventsMaker.FieldName.SOURCE, "test_ce_source");
            ceValueStruct.put(CloudEventsMaker.FieldName.SPECVERSION, "1.0");
            ceValueStruct.put(CloudEventsMaker.FieldName.TYPE, "TestType");
            ceValueStruct.put(CloudEventsMaker.FieldName.TIME, LocalDateTime.now().toString());
            ceValueStruct.put(CloudEventsMaker.FieldName.DATACONTENTTYPE, "application/json");
            ceValueStruct.put(CloudEventsMaker.FieldName.DATA, baseRecord.value());

            final Object ceValue;
            if (cloudEventsSerializerType == SerializerType.JSON) {
                ceValue = convertCloudEventToMap(ceSchema, ceValueStruct);
                ceSchema = SchemaBuilder.map(SchemaBuilder.string(), SchemaBuilder.string()).name(schemaName).build();
            }
            else {
                ceValue = ceValueStruct;
            }

            var sessionFactory = config.getHibernateConfiguration().buildSessionFactory();
            var databaseDialect = DatabaseDialectResolver.resolve(config, sessionFactory);

            return new JdbcKafkaSinkRecord(new SinkRecord(baseRecord.topic(), baseRecord.kafkaPartition(), baseRecord.keySchema(), baseRecord.key(),
                    ceSchema, ceValue,
                    baseRecord.kafkaOffset(), baseRecord.timestamp(), baseRecord.timestampType(), baseRecord.headers()),
                    config.getPrimaryKeyMode(),
                    config.getPrimaryKeyFields(),
                    config.getFieldFilter(),
                    cloudEventsSchemaNamePattern,
                    databaseDialect);
        }

        private Envelope createEnvelope() {
            return Envelope.defineSchema()
                    .withRecord(recordSchema)
                    .withSource(sourceSchema)
                    .withName((Strings.isNullOrBlank(name) ? "dummy" : name) + ".Envelope")
                    .build();
        }

        private Struct populateStructFromMap(Struct struct, Map<String, Object> values) {
            for (Map.Entry<String, Object> entry : values.entrySet()) {
                struct.put(entry.getKey(), entry.getValue());
            }
            return struct;
        }

        private Struct populateStructForKey() {
            if (keySchema != null) {
                return populateStructFromMap(new Struct(keySchema), keyValues);
            }
            return null;
        }

        private Map<String, Object> convertCloudEventToMap(Schema ceSchema, Struct ceValueStruct) {
            byte[] cloudEventJson;
            try (JsonConverter jsonConverter = new JsonConverter()) {
                final Map<String, Object> jsonDataConverterConfig = new HashMap<>();
                jsonDataConverterConfig.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
                jsonDataConverterConfig.put(JsonConverterConfig.TYPE_CONFIG, "value");
                jsonConverter.configure(jsonDataConverterConfig);

                cloudEventJson = jsonConverter.fromConnectData(null, ceSchema, ceValueStruct);
            }

            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> map;
            try {
                map = objectMapper.readValue(cloudEventJson, new TypeReference<>() {
                });
            }
            catch (IOException e) {
                throw new DataException("Failed to instantiate map from CloudEvent JSON");
            }
            final Object dataMap = map.get(CloudEventsMaker.FieldName.DATA);
            if (dataMap != null) {
                map.put(CloudEventsMaker.FieldName.DATA, dataMap);
            }
            return map;
        }
    }

    private enum Type {
        CREATE,
        UPDATE,
        DELETE,
        TOMBSTONE,
        TRUNCATE,
        CLOUD_EVENT
    }
}
