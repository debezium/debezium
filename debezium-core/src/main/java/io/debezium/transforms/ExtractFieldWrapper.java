package io.debezium.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMapOrNull;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStructOrNull;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public abstract class ExtractFieldWrapper<R extends ConnectRecordWrapper<R>> implements TransformationWrapper<R> {

    public static final String OVERVIEW_DOC = "Extract the specified field from a Struct when schema present, or a Map in the case of schemaless data. "
            + "Any null values are passed through unmodified."
            + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getName() + "</code>) "
            + "or value (<code>" + Value.class.getName() + "</code>).";

    private static final String FIELD_CONFIG = "field";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.MEDIUM, "Field name to extract.");

    private static final String PURPOSE = "field extraction";

    private String fieldName;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString(FIELD_CONFIG);
    }

    @Override
    public R apply(R record) {
        final Schema schema = operatingSchema(record);
        if (schema == null) {
            final Map<String, Object> value = requireMapOrNull(operatingValue(record), PURPOSE);
            return newRecord(record, null, value == null ? null : value.get(fieldName));
        }
        else {
            final Struct value = requireStructOrNull(operatingValue(record), PURPOSE);
            Field field = schema.field(fieldName);

            if (field == null) {
                throw new IllegalArgumentException("Unknown field: " + fieldName);
            }

            return newRecord(record, field.schema(), value == null ? null : value.get(fieldName));
        }
    }

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecordWrapper<R>> extends ExtractFieldWrapper<R> {
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

    public static class Value<R extends ConnectRecordWrapper<R>> extends ExtractFieldWrapper<R> {
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
