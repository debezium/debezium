package io.debezium.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStructOrNull;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public abstract class FlattenWrapper<R extends ConnectRecordWrapper<R>> implements TransformationWrapper<R> {

    public static final String OVERVIEW_DOC = "Flatten a nested data structure, generating names for each field by concatenating the field names at each "
            + "level with a configurable delimiter character. Applies to Struct when schema present, or a Map "
            + "in the case of schemaless data. The default delimiter is '.'."
            + "<p/>Use the concrete transformation type designed for the record key (<code>" + org.apache.kafka.connect.transforms.Flatten.Key.class.getName()
            + "</code>) "
            + "or value (<code>" + org.apache.kafka.connect.transforms.Flatten.Value.class.getName() + "</code>).";

    private static final String DELIMITER_CONFIG = "delimiter";
    private static final String DELIMITER_DEFAULT = ".";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(DELIMITER_CONFIG, ConfigDef.Type.STRING, DELIMITER_DEFAULT, ConfigDef.Importance.MEDIUM,
                    "Delimiter to insert between field names from the input record when generating field names for the "
                            + "output record");

    private static final String PURPOSE = "flattening";

    private String delimiter;

    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        delimiter = config.getString(DELIMITER_CONFIG);
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

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        final Map<String, Object> newValue = new LinkedHashMap<>();
        applySchemaless(value, "", newValue);
        return newRecord(record, null, newValue);
    }

    private void applySchemaless(Map<String, Object> originalRecord, String fieldNamePrefix, Map<String, Object> newRecord) {
        for (Map.Entry<String, Object> entry : originalRecord.entrySet()) {
            final String fieldName = fieldName(fieldNamePrefix, entry.getKey());
            Object value = entry.getValue();
            if (value == null) {
                newRecord.put(fieldName(fieldNamePrefix, entry.getKey()), null);
                return;
            }

            Schema.Type inferredType = ConnectSchema.schemaType(value.getClass());
            if (inferredType == null) {
                throw new DataException("Flatten transformation was passed a value of type " + value.getClass()
                        + " which is not supported by Connect's data API");
            }
            switch (inferredType) {
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                case FLOAT32:
                case FLOAT64:
                case BOOLEAN:
                case STRING:
                case BYTES:
                    newRecord.put(fieldName(fieldNamePrefix, entry.getKey()), entry.getValue());
                    break;
                case MAP:
                    final Map<String, Object> fieldValue = requireMap(entry.getValue(), PURPOSE);
                    applySchemaless(fieldValue, fieldName, newRecord);
                    break;
                default:
                    throw new DataException("Flatten transformation does not support " + entry.getValue().getClass()
                            + " for record without schemas (for field " + fieldName + ").");
            }
        }
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStructOrNull(operatingValue(record), PURPOSE);

        Schema schema = operatingSchema(record);
        Schema updatedSchema = schemaUpdateCache.get(schema);
        if (updatedSchema == null) {
            final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
            Struct defaultValue = (Struct) schema.defaultValue();
            buildUpdatedSchema(schema, "", builder, schema.isOptional(), defaultValue);
            updatedSchema = builder.build();
            schemaUpdateCache.put(schema, updatedSchema);
        }
        if (value == null) {
            return newRecord(record, updatedSchema, null);
        }
        else {
            final Struct updatedValue = new Struct(updatedSchema);
            buildWithSchema(value, "", updatedValue);
            return newRecord(record, updatedSchema, updatedValue);
        }
    }

    /**
     * Build an updated Struct Schema which flattens all nested fields into a single struct, handling cases where
     * optionality and default values of the flattened fields are affected by the optionality and default values of
     * parent/ancestor schemas (e.g. flattened field is optional because the parent schema was optional, even if the
     * schema itself is marked as required).
     * @param schema the schema to translate
     * @param fieldNamePrefix the prefix to use on field names, i.e. the delimiter-joined set of ancestor field names
     * @param newSchema the flattened schema being built
     * @param optional true if any ancestor schema is optional
     * @param defaultFromParent the default value, if any, included via the parent/ancestor schemas
     */
    private void buildUpdatedSchema(Schema schema, String fieldNamePrefix, SchemaBuilder newSchema, boolean optional, Struct defaultFromParent) {
        for (Field field : schema.fields()) {
            final String fieldName = fieldName(fieldNamePrefix, field.name());
            final boolean fieldIsOptional = optional || field.schema().isOptional();
            Object fieldDefaultValue = null;
            if (field.schema().defaultValue() != null) {
                fieldDefaultValue = field.schema().defaultValue();
            }
            else if (defaultFromParent != null) {
                fieldDefaultValue = defaultFromParent.get(field);
            }
            switch (field.schema().type()) {
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                case FLOAT32:
                case FLOAT64:
                case BOOLEAN:
                case STRING:
                case BYTES:
                    newSchema.field(fieldName, convertFieldSchema(field.schema(), fieldIsOptional, fieldDefaultValue));
                    break;
                case STRUCT:
                    buildUpdatedSchema(field.schema(), fieldName, newSchema, fieldIsOptional, (Struct) fieldDefaultValue);
                    break;
                default:
                    throw new DataException("Flatten transformation does not support " + field.schema().type()
                            + " for record with schemas (for field " + fieldName + ").");
            }
        }
    }

    /**
     * Convert the schema for a field of a Struct with a primitive schema to the schema to be used for the flattened
     * version, taking into account that we may need to override optionality and default values in the flattened version
     * to take into account the optionality and default values of parent/ancestor schemas
     * @param orig the original schema for the field
     * @param optional whether the new flattened field should be optional
     * @param defaultFromParent the default value either taken from the existing field or provided by the parent
     */
    private Schema convertFieldSchema(Schema orig, boolean optional, Object defaultFromParent) {
        // Note that we don't use the schema translation cache here. It might save us a bit of effort, but we really
        // only care about caching top-level schema translations.

        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(orig);
        if (optional)
            builder.optional();
        if (defaultFromParent != null)
            builder.defaultValue(defaultFromParent);
        return builder.build();
    }

    private void buildWithSchema(Struct record, String fieldNamePrefix, Struct newRecord) {
        if (record == null) {
            return;
        }
        for (Field field : record.schema().fields()) {
            final String fieldName = fieldName(fieldNamePrefix, field.name());
            switch (field.schema().type()) {
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                case FLOAT32:
                case FLOAT64:
                case BOOLEAN:
                case STRING:
                case BYTES:
                    newRecord.put(fieldName, record.get(field));
                    break;
                case STRUCT:
                    buildWithSchema(record.getStruct(field.name()), fieldName, newRecord);
                    break;
                default:
                    throw new DataException("Flatten transformation does not support " + field.schema().type()
                            + " for record with schemas (for field " + fieldName + ").");
            }
        }
    }

    private String fieldName(String prefix, String fieldName) {
        return prefix.isEmpty() ? fieldName : (prefix + delimiter + fieldName);
    }

    public static class Key<R extends ConnectRecordWrapper<R>> extends FlattenWrapper<R> {
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

    public static class Value<R extends ConnectRecordWrapper<R>> extends FlattenWrapper<R> {
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
