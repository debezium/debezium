package io.debezium.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.*;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public abstract class InsertFieldWrapper<R extends ConnectRecordWrapper<R>> implements TransformationWrapper<R> {

    public static final String OVERVIEW_DOC = "Insert field(s) using attributes from the record metadata or a configured static value."
            + "<p/>Use the concrete transformation type designed for the record key (<code>" + InsertFieldWrapper.Key.class.getName() + "</code>) "
            + "or value (<code>" + InsertFieldWrapper.Value.class.getName() + "</code>).";

    private interface ConfigName {
        String TOPIC_FIELD = "topic.field";
        String PARTITION_FIELD = "partition.field";
        String OFFSET_FIELD = "offset.field";
        String TIMESTAMP_FIELD = "timestamp.field";
        String STATIC_FIELD = "static.field";
        String STATIC_VALUE = "static.value";
    }

    private static final String OPTIONALITY_DOC = "Suffix with <code>!</code> to make this a required field, or <code>?</code> to keep it optional (the default).";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(InsertFieldWrapper.ConfigName.TOPIC_FIELD, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Field name for Kafka topic. " + OPTIONALITY_DOC)
            .define(InsertFieldWrapper.ConfigName.PARTITION_FIELD, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Field name for Kafka partition. " + OPTIONALITY_DOC)
            .define(InsertFieldWrapper.ConfigName.OFFSET_FIELD, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Field name for Kafka offset - only applicable to sink connectors.<br/>" + OPTIONALITY_DOC)
            .define(InsertFieldWrapper.ConfigName.TIMESTAMP_FIELD, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Field name for record timestamp. " + OPTIONALITY_DOC)
            .define(InsertFieldWrapper.ConfigName.STATIC_FIELD, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Field name for static data field. " + OPTIONALITY_DOC)
            .define(InsertFieldWrapper.ConfigName.STATIC_VALUE, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Static field value, if field name configured.");

    private static final String PURPOSE = "field insertion";

    private static final Schema OPTIONAL_TIMESTAMP_SCHEMA = Timestamp.builder().optional().build();

    private static final class InsertionSpec {
        final String name;
        final boolean optional;

        private InsertionSpec(String name, boolean optional) {
            this.name = name;
            this.optional = optional;
        }

        public static InsertFieldWrapper.InsertionSpec parse(String spec) {
            if (spec == null)
                return null;
            if (spec.endsWith("?")) {
                return new InsertFieldWrapper.InsertionSpec(spec.substring(0, spec.length() - 1), true);
            }
            if (spec.endsWith("!")) {
                return new InsertFieldWrapper.InsertionSpec(spec.substring(0, spec.length() - 1), false);
            }
            return new InsertFieldWrapper.InsertionSpec(spec, true);
        }
    }

    private InsertFieldWrapper.InsertionSpec topicField;
    private InsertFieldWrapper.InsertionSpec partitionField;
    private InsertFieldWrapper.InsertionSpec offsetField;
    private InsertFieldWrapper.InsertionSpec timestampField;
    private InsertFieldWrapper.InsertionSpec staticField;
    private String staticValue;

    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        topicField = InsertFieldWrapper.InsertionSpec.parse(config.getString(InsertFieldWrapper.ConfigName.TOPIC_FIELD));
        partitionField = InsertFieldWrapper.InsertionSpec.parse(config.getString(InsertFieldWrapper.ConfigName.PARTITION_FIELD));
        offsetField = InsertFieldWrapper.InsertionSpec.parse(config.getString(InsertFieldWrapper.ConfigName.OFFSET_FIELD));
        timestampField = InsertFieldWrapper.InsertionSpec.parse(config.getString(InsertFieldWrapper.ConfigName.TIMESTAMP_FIELD));
        staticField = InsertFieldWrapper.InsertionSpec.parse(config.getString(InsertFieldWrapper.ConfigName.STATIC_FIELD));
        staticValue = config.getString(InsertFieldWrapper.ConfigName.STATIC_VALUE);

        if (topicField == null && partitionField == null && offsetField == null && timestampField == null && staticField == null) {
            throw new ConfigException("No field insertion configured");
        }

        if (staticField != null && staticValue == null) {
            throw new ConfigException(InsertFieldWrapper.ConfigName.STATIC_VALUE, null, "No value specified for static field: " + staticField);
        }

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    @Override
    public R apply(R record) {
        if (operatingValue(record) == null) {
            return record;
        }
        else if (operatingSchema(record) == null) {
            return applySchemaless(record);
        }
        else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

        final Map<String, Object> updatedValue = new HashMap<>(value);

        if (topicField != null) {
            updatedValue.put(topicField.name, record.topic());
        }
        if (partitionField != null && record.kafkaPartition() != null) {
            updatedValue.put(partitionField.name, record.kafkaPartition());
        }
        if (offsetField != null) {
            updatedValue.put(offsetField.name, requireSinkRecord(record, PURPOSE).kafkaOffset());
        }
        if (timestampField != null && record.timestamp() != null) {
            updatedValue.put(timestampField.name, record.timestamp());
        }
        if (staticField != null && staticValue != null) {
            updatedValue.put(staticField.name, staticValue);
        }

        return newRecord(record, null, updatedValue);
    }

    SinkRecord requireSinkRecord(ConnectRecordWrapper<?> record, String purpose) {
        if (!(record instanceof SinkRecord)) {
            throw new DataException("Only SinkRecord supported for [" + purpose + "], found: " + nullSafeClassName(record));
        }
        return (SinkRecord) record;
    }

    String nullSafeClassName(Object x) {
        return x == null ? "null" : x.getClass().getName();
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }

        if (topicField != null) {
            updatedValue.put(topicField.name, record.topic());
        }
        if (partitionField != null && record.kafkaPartition() != null) {
            updatedValue.put(partitionField.name, record.kafkaPartition());
        }
        if (offsetField != null) {
            updatedValue.put(offsetField.name, requireSinkRecord(record, PURPOSE).kafkaOffset());
        }
        if (timestampField != null && record.timestamp() != null) {
            updatedValue.put(timestampField.name, new Date(record.timestamp()));
        }
        if (staticField != null && staticValue != null) {
            updatedValue.put(staticField.name, staticValue);
        }

        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }

        if (topicField != null) {
            builder.field(topicField.name, topicField.optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA);
        }
        if (partitionField != null) {
            builder.field(partitionField.name, partitionField.optional ? Schema.OPTIONAL_INT32_SCHEMA : Schema.INT32_SCHEMA);
        }
        if (offsetField != null) {
            builder.field(offsetField.name, offsetField.optional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA);
        }
        if (timestampField != null) {
            builder.field(timestampField.name, timestampField.optional ? OPTIONAL_TIMESTAMP_SCHEMA : Timestamp.SCHEMA);
        }
        if (staticField != null) {
            builder.field(staticField.name, staticField.optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA);
        }

        return builder.build();
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecordWrapper<R>> extends InsertFieldWrapper<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecordWrapper<R>> extends InsertFieldWrapper<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }

    }

}
