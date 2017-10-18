
/*
Note: All the null values for any fields should be made to optional, if not while validating the value and schema, we would end up getting null value for field error.
       Going to convert all null value columns into isoptional if they dont contains any data, so to avoid any kinds of (Conversion error: null value for field that is required and has no default value),
       not just for nestedvalues, but also for struct fields.
 */

package org.telmate.SMT;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.Date;
import java.util.stream.Stream;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class DebeziumTimestampConverter<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Convert Debezium timestamps between different formats such as Unix epoch, strings, and Connect Date/Timestamp types."
                    + "Applies to org.apache.kafka.connect.data.Struct fields or individual fields or to the entire value."
                    + "<p/>Use the concrete transformation type designed for the record key (<code>" + DebeziumTimestampConverter.Key.class.getName() + "</code>) "
                    + "or value (<code>" + DebeziumTimestampConverter.Value.class.getName() + "</code>).";

    public static final String FIELD_CONFIG = "field";
    private static final String FIELD_DEFAULT = "";

    public static final String TARGET_TYPE_CONFIG = "target.type";

    public static final String TIMESTAMP_FORMAT_CONFIG = "timestamp.format";
    private static final String TIMESTAMP_FORMAT_DEFAULT = "yyyy-MM-dd HH:mm:ss";

    public static final String DATE_FORMAT_CONFIG = "date.format";
    private static final String DATE_FORMAT_DEFAULT = "yyyy-MM-dd";

    public static final String FIELD_TYPE_CONFIG = "field.type";
    private static final String FIELD_TYPE_DEFAULT = "empty.value";

    public static final String STRUCT_FIELD_NAME_CONFIG = "struct.field";
    private static final String STRUCT_FIELD_NAME_DEFAULT = "";


    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_CONFIG, ConfigDef.Type.STRING, FIELD_DEFAULT, ConfigDef.Importance.LOW,
                    "The field containing the timestamp, or empty if the entire value is a timestamp")
            .define(TARGET_TYPE_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.LOW,
                    "The desired timestamp representation: string, unix, Date, Time, or Timestamp")
            .define(TIMESTAMP_FORMAT_CONFIG, ConfigDef.Type.STRING, TIMESTAMP_FORMAT_DEFAULT, ConfigDef.Importance.LOW,
                    "A SimpleDateFormat-compatible format for the timestamp. Used to generate the output when type=string "
                            + "or used to parse the input if the input is a string.")
            .define(DATE_FORMAT_CONFIG, ConfigDef.Type.STRING, DATE_FORMAT_DEFAULT, ConfigDef.Importance.LOW,
                    "A SimpleDateFormat-compatible format for the date. Used to generate the output when type=string "
                            + "or used to parse the input if the input is a string.")
            .define(FIELD_TYPE_CONFIG, ConfigDef.Type.STRING, FIELD_TYPE_DEFAULT, ConfigDef.Importance.LOW, "is a comma seperated string, providing an option to add multiple converters.\n" +
                    "<Type of Timestamp Object> -> <To Target type>\n" +
                    "can add multiple type converters as below," +
                    " <Type of Timestamp Object> -> <To Target type>,<Type of Timestamp Object> -> <To Target type>")
            .define(STRUCT_FIELD_NAME_CONFIG, ConfigDef.Type.STRING, STRUCT_FIELD_NAME_DEFAULT, ConfigDef.Importance.LOW, "Field name that is only to be modified in Nested Struct");

    private static final String PURPOSE = "converting timestamp formats";
    private static final String TYPE_INT64 = "INT64";
    private static final String TYPE_INT32 = "INT32";
    private static final String TYPE_INT16 = "INT16";
    private static final String TYPE_INT8 = "INT8";
    private static final String TYPE_FLOAT32 = "FLOAT32";
    private static final String TYPE_FLOAT64 = "FLOAT64";
    private static final String TYPE_BOOLEAN = "BOOLEAN";
    private static final String STRING_TYPE = "STRING";
    private static final String TYPE_BYTES = "BYTES";
    private static final String TYPE_STRUCT = "STRUCT";

    private static final String TYPE_STRING = "string";
    private static final String TYPE_UNIX = "unix";
    private static final String TYPE_DATE = "Date";
    private static final String TYPE_TIME = "Time";
    private static final String TYPE_TIMESTAMP = "Timestamp";
    private static final String TYPE_EPCOH_DATE = "EpochDate";
    private static final Set<String> VALID_TYPES = new HashSet<>(Arrays.asList(TYPE_STRING, TYPE_UNIX, TYPE_DATE, TYPE_TIME, TYPE_TIMESTAMP, TYPE_EPCOH_DATE));

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    private static final Map<String, TimestampTranslator> TRANSLATORS = new HashMap<>();
    private static final Map<String, OptionalTypeFieldSchema> OPTIONAL_TRANSLATORS = new HashMap<>();

    static {
        TRANSLATORS.put(TYPE_STRING, new TimestampTranslator() {
            @Override
            public Date toRaw(Config config, Object orig) {
                if (!(orig instanceof String))
                    throw new DataException("Expected string timestamp to be a String, but found " + orig.getClass());
                try {
                    return config.format.parse((String) orig);
                } catch (ParseException e) {
                    throw new DataException("Could not parse timestamp: value (" + orig + ") does not match pattern ("
                            + config.format.toPattern() + ")", e);
                }
            }

            @Override
            public Schema typeSchema() {
                return Schema.STRING_SCHEMA;
            }

            @Override
            public String toType(Config config, Date orig,String format) {
                synchronized (config.format) {
                    if (format==DATE_FORMAT_CONFIG){
                        return config.dateFormat.format(orig);
                    }else {
                        return config.format.format(orig);
                    }
                }
            }

            @Override
            public Schema optionalSchema() {
                return Schema.OPTIONAL_STRING_SCHEMA.schema();
            }
        });

        TRANSLATORS.put(TYPE_UNIX, new TimestampTranslator() {
            @Override
            public Date toRaw(Config config, Object orig) {
                if (!(orig instanceof Long))
                    throw new DataException("Expected Unix timestamp to be a Long, but found " + orig.getClass());
                return Timestamp.toLogical(Timestamp.SCHEMA, (Long) orig);
            }

            @Override
            public Schema typeSchema() {
                return Schema.INT64_SCHEMA;
            }

            @Override
            public Long toType(Config config, Date orig,String format) {
                return Timestamp.fromLogical(Timestamp.SCHEMA, orig);
            }

            @Override
            public Schema optionalSchema() {
                return Schema.OPTIONAL_INT64_SCHEMA.schema();
            }
        });

        TRANSLATORS.put(TYPE_DATE, new TimestampTranslator() {
            @Override
            public Date toRaw(Config config, Object orig) {
                if (!(orig instanceof Date))
                    throw new DataException("Expected Date to be a java.util.Date, but found " + orig.getClass());
                // Already represented as a java.util.Date and Connect Dates are a subset of valid java.util.Date values
                return (Date) orig;
            }

            @Override
            public Schema typeSchema() {
                return org.apache.kafka.connect.data.Date.SCHEMA;
            }

            @Override
            public Date toType(Config config, Date orig,String format) {
                Calendar result = Calendar.getInstance(UTC);
                result.setTime(orig);
                result.set(Calendar.HOUR_OF_DAY, 0);
                result.set(Calendar.MINUTE, 0);
                result.set(Calendar.SECOND, 0);
                result.set(Calendar.MILLISECOND, 0);
                return result.getTime();
            }

            @Override
            public Schema optionalSchema() {
                return SchemaBuilder.int32()
                        .name(org.apache.kafka.connect.data.Date.LOGICAL_NAME)
                        .version(1).optional().build().schema();
            }
        });

        TRANSLATORS.put(TYPE_TIME, new TimestampTranslator() {
            @Override
            public Date toRaw(Config config, Object orig) {
                if (!(orig instanceof Date))
                    throw new DataException("Expected Time to be a java.util.Date, but found " + orig.getClass());
                // Already represented as a java.util.Date and Connect Times are a subset of valid java.util.Date values
                return (Date) orig;
            }

            @Override
            public Schema typeSchema() {
                return Time.SCHEMA;
            }

            @Override
            public Date toType(Config config, Date orig,String format) {
                Calendar origCalendar = Calendar.getInstance(UTC);
                origCalendar.setTime(orig);
                Calendar result = Calendar.getInstance(UTC);
                result.setTimeInMillis(0L);
                result.set(Calendar.HOUR_OF_DAY, origCalendar.get(Calendar.HOUR_OF_DAY));
                result.set(Calendar.MINUTE, origCalendar.get(Calendar.MINUTE));
                result.set(Calendar.SECOND, origCalendar.get(Calendar.SECOND));
                result.set(Calendar.MILLISECOND, origCalendar.get(Calendar.MILLISECOND));
                return result.getTime();
            }

            @Override
            public Schema optionalSchema() {
                return SchemaBuilder.int32()
                        .name(org.apache.kafka.connect.data.Time.LOGICAL_NAME)
                        .version(1).optional().build().schema();
            }
        });

        TRANSLATORS.put(TYPE_TIMESTAMP, new TimestampTranslator() {
            @Override
            public Date toRaw(Config config, Object orig) {
                if (!(orig instanceof Date))
                    throw new DataException("Expected Timestamp to be a java.util.Date, but found " + orig.getClass());
                return (Date) orig;
            }

            @Override
            public Schema typeSchema() {
                return Timestamp.SCHEMA;
            }

            @Override
            public Date toType(Config config, Date orig,String format) {
                return orig;
            }

            @Override
            public Schema optionalSchema() {
                return SchemaBuilder.int64()
                        .name(org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME)
                        .version(1).optional().build().schema();
            }
        });

        TRANSLATORS.put(TYPE_EPCOH_DATE, new TimestampTranslator() {
            @Override
            public Date toRaw(Config config, Object orig) {
                return Date.from(java.time.LocalDate.ofEpochDay((Integer) orig).atStartOfDay(ZoneId.systemDefault()).toInstant());
            }

            @Override
            public Schema typeSchema() {
                return org.apache.kafka.connect.data.Date.SCHEMA;
            }

            @Override
            public Object toType(Config config, Date orig,String format) {
                Calendar origCalendar = Calendar.getInstance(UTC);
                origCalendar.setTime(orig);
                Calendar result = Calendar.getInstance(UTC);
                result.setTimeInMillis(0L);
                result.set(Calendar.HOUR_OF_DAY, origCalendar.get(Calendar.HOUR_OF_DAY));
                result.set(Calendar.MINUTE, origCalendar.get(Calendar.MINUTE));
                result.set(Calendar.SECOND, origCalendar.get(Calendar.SECOND));
                result.set(Calendar.MILLISECOND, origCalendar.get(Calendar.MILLISECOND));
                return result.getTime();
            }

            @Override
            public Schema optionalSchema() {
                return SchemaBuilder.int32()
                        .name(org.apache.kafka.connect.data.Date.LOGICAL_NAME)
                        .version(1).optional().build().schema();
            }
        });

        OPTIONAL_TRANSLATORS.put(TYPE_INT64, () -> {
            return Schema.OPTIONAL_INT64_SCHEMA.schema();
        });

        OPTIONAL_TRANSLATORS.put(TYPE_INT32, () -> {
            return Schema.OPTIONAL_INT32_SCHEMA.schema();
        });

        OPTIONAL_TRANSLATORS.put(TYPE_INT16, () -> {
            return Schema.OPTIONAL_INT16_SCHEMA.schema();
        });

        OPTIONAL_TRANSLATORS.put(TYPE_INT8, () -> {
            return Schema.OPTIONAL_INT8_SCHEMA.schema();
        });

        OPTIONAL_TRANSLATORS.put(TYPE_FLOAT64, () -> {
            return Schema.OPTIONAL_FLOAT64_SCHEMA.schema();
        });

        OPTIONAL_TRANSLATORS.put(TYPE_FLOAT32, () -> {
            return Schema.OPTIONAL_FLOAT32_SCHEMA.schema();
        });

        OPTIONAL_TRANSLATORS.put(TYPE_BOOLEAN, () -> {
            return Schema.OPTIONAL_BOOLEAN_SCHEMA.schema();
        });

        OPTIONAL_TRANSLATORS.put(STRING_TYPE, () -> {
            return Schema.OPTIONAL_STRING_SCHEMA.schema();
        });

        OPTIONAL_TRANSLATORS.put(TYPE_BYTES, () -> {
            return Schema.OPTIONAL_BYTES_SCHEMA.schema();
        });

        OPTIONAL_TRANSLATORS.put(TYPE_STRUCT, () -> {
            return SchemaBuilder.type(Schema.Type.STRUCT).optional().build().schema();
        });

    }


    public static Boolean containsField(String[] field_type, String field_name) {
        return Stream.of(field_type).anyMatch(f -> f.equals(field_name));
    }

    // This is a bit unusual, but allows the transformation config to be passed to static anonymous classes to customize
    // their behavior
    public static class Config {
        Config(String field, String type, SimpleDateFormat format,SimpleDateFormat dateFormat, String[] field_type, String[] struct_field) {
            this.field = field;
            this.type = type;
            this.format = format;
            this.dateFormat = dateFormat;
            this.field_type = field_type;
            this.struct_field = struct_field;
        }

        String field;
        String type;
        SimpleDateFormat format;
        SimpleDateFormat dateFormat;
        String[] field_type;
        String[] struct_field;
    }

    private Config config;
    private Cache<Schema, Schema> schemaUpdateCache;
    private static final Map<String, String> TypeConverters = new HashMap<>();

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig simpleConfig = new SimpleConfig(CONFIG_DEF, configs);
        final String field = simpleConfig.getString(FIELD_CONFIG);
        final String type = simpleConfig.getString(TARGET_TYPE_CONFIG);
        final String[] field_type = simpleConfig.getString(FIELD_TYPE_CONFIG).split(",");
        final String[] struct_field = simpleConfig.getString(STRUCT_FIELD_NAME_CONFIG).split(",");
        String[] structTypeList = new String[]{type};
        String formatPatternTimestamp = simpleConfig.getString(TIMESTAMP_FORMAT_CONFIG);
        String formatPatternDate = simpleConfig.getString(DATE_FORMAT_CONFIG);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));

        if (!field_type[0].equals(FIELD_TYPE_DEFAULT)) {
            for (String typeMatch : field_type) {
                TypeConverters.put(typeMatch.split("->")[0], typeMatch.split("->")[1]);
            }
            structTypeList = Arrays.copyOf(TypeConverters.values().toArray(), TypeConverters.values().toArray().length, String[].class);
        }

        for (String typeCheck : structTypeList) {
            if (!VALID_TYPES.contains(typeCheck)) {
                throw new ConfigException("Unknown timestamp type in TimestampConverter: " + typeCheck + ". Valid values are "
                        + Utils.join(VALID_TYPES, ", ") + ".");
            }
            if (typeCheck.equals(TYPE_STRING) && formatPatternTimestamp.trim().isEmpty()) {
                throw new ConfigException("TimestampConverter requires format option to be specified when using string timestamps");
            }
            if (typeCheck.equals(TYPE_STRING) && formatPatternDate.trim().isEmpty()) {
                throw new ConfigException("TimestampConverter requires format option to be specified when using string dates");
            }
        }

        SimpleDateFormat format = null;
        SimpleDateFormat dateFormat = null;
        if (formatPatternTimestamp != null && !formatPatternTimestamp.trim().isEmpty()) {
            try {
                format = new SimpleDateFormat(formatPatternTimestamp);
                format.setTimeZone(UTC);
            } catch (IllegalArgumentException e) {
                throw new ConfigException("TimestampConverter requires a SimpleDateFormat-compatible pattern for string timestamps: "
                        + formatPatternTimestamp, e);
            }
        }
        if (formatPatternDate != null && !formatPatternDate.trim().isEmpty()) {
            try {
                dateFormat = new SimpleDateFormat(formatPatternDate);
                dateFormat.setTimeZone(UTC);
            } catch (IllegalArgumentException e) {
                throw new ConfigException("TimestampConverter requires a SimpleDateFormat-compatible pattern for string dates: "
                        + formatPatternDate, e);
            }
        }
        config = new Config(field, type, format, dateFormat, field_type, struct_field);
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    public static class Key<R extends ConnectRecord<R>> extends DebeziumTimestampConverter<R> {
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

    public static class Value<R extends ConnectRecord<R>> extends DebeziumTimestampConverter<R> {
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

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);


    /**
     * Creates the new schema of the nested struct fields that matches with config.field_type.
     * Assign optional fields to the fields that does not contain value or null value.
     *
     * @param field the input nested Struct field.
     * @param value the input nested Struct field value.
     **/
    private SchemaBuilder NestedStructSchemaUpdate(Field field, Struct value) {
        SchemaBuilder builder = SchemaUtil.copySchemaBasics(field.schema(), SchemaBuilder.struct());
        for (Field structField : field.schema().fields()) {
            String structFieldSchemaName = structField.schema().name();
            final Object nestedStructField = ((Struct) value.get(field)).get(structField);
            if (structFieldSchemaName != null && !structFieldSchemaName.isEmpty() && TypeConverters.containsKey(structFieldSchemaName)) {
                if (nestedStructField == null || nestedStructField.equals("")) {
                    builder.field(structField.name(), TRANSLATORS.get(TypeConverters.get(structFieldSchemaName)).optionalSchema());
                } else {
                    builder.field(structField.name(), TRANSLATORS.get(TypeConverters.get(structFieldSchemaName)).typeSchema());
                }
            } else {
                if (nestedStructField == null || nestedStructField.equals("")) {
                    builder.field(structField.name(), OPTIONAL_TRANSLATORS.get(structField.schema().type().toString()).optionalSchemaNonTimestamp());
                } else {
                    builder.field(structField.name(), structField.schema());
                }
            }
        }
        return builder;
    }

    /**
     * Creates the new field value of the nested struct that matches with config.field_type.
     * Avoids all the null value fields and converts the desired field value that matches to the config parameter field_type.
     *
     * @param field               the input nested Struct field.
     * @param value               the input nested Struct field value.
     * @param nestedUpdatedSchema updated field schema with target formats.
     */
    private Struct NestedStructFieldValueUpdate(Field field, Struct value, Field nestedUpdatedSchema) {
        Struct updatedNestedStructValue = new Struct(nestedUpdatedSchema.schema());
        for (Field nestedField : ((Struct) value.get(field)).schema().fields()) { //try to convert ((Struct) value.get(field)).get(nestedField) into a single field rather than computing every single time
            final Object updatedNestedFieldValue;
            final Object nestedFieldValue = ((Struct) value.get(field)).get(nestedField);
            if (nestedFieldValue == null || nestedFieldValue.equals("")) {
                continue;
            }
            if (nestedField.schema().name() != null && !nestedField.schema().name().isEmpty() && TypeConverters.containsKey(nestedField.schema().name()) && (nestedFieldValue != null && !nestedFieldValue.equals(""))) {
                updatedNestedFieldValue = convertTimestamp(nestedFieldValue, timestampTypeFromSchema(nestedField.schema()), nestedField);
            } else {
                updatedNestedFieldValue = nestedFieldValue;
            }
            updatedNestedStructValue.put(nestedField.name(), updatedNestedFieldValue);
        }
        return updatedNestedStructValue;
    }

    private R applyWithSchema(R record) {
        final Schema schema = operatingSchema(record);
        if (config.field.isEmpty() && config.field_type.length == 0) {
            Object value = operatingValue(record);
            // New schema is determined by the requested target timestamp type
            Schema updatedSchema = TRANSLATORS.get(config.type).typeSchema();
            return newRecord(record, updatedSchema, convertTimestamp(value, timestampTypeFromSchema(schema), (Field) value));
        } else {
            final Struct value = requireStruct(operatingValue(record), PURPOSE);
            Schema updatedSchema = schemaUpdateCache.get(value.schema());
            if (updatedSchema == null) {
                SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
                for (Field field : schema.fields()) {
                    if (value.get(field) == null || value.get(field).equals("")) {
                        builder.field(field.name(), field.schema()).optional();
                        continue;
                    }

                    if ((!(config.field_type.length == 0) && !(config.struct_field.length == 0)
                            && containsField(config.struct_field, field.name())
                            && value.get(field) != null)) {
                        /**
                         (FieldType) is not empty, which confirms that transformation is to be made to Struct type, which has nested field of columntype.
                         (Nested struct column type change.)
                         */
                        SchemaBuilder nestedStructBuilder = NestedStructSchemaUpdate(field, value);
                        builder.field(field.name(), nestedStructBuilder.schema());
                    } else if (field.name().equals(config.field)) {
                        /**
                         if config field alone is provided, it implies that it is not nested struct and changes the field type accordingly
                         (Field change on non nested data.)
                         */
                        builder.field(field.name(), TRANSLATORS.get(config.type).typeSchema());
                    } else {
                        builder.field(field.name(), field.schema());
                    }
                }
                if (schema.isOptional())
                    builder.optional();
                if (schema.defaultValue() != null) {
                    Struct updatedDefaultValue = applyValueWithSchema((Struct) schema.defaultValue(), builder);
                    builder.defaultValue(updatedDefaultValue);
                }

                updatedSchema = builder.build();
            }

            Struct updatedValue = applyValueWithSchema(value, updatedSchema);
            return newRecord(record, updatedSchema, updatedValue);
        }
    }

    private Struct applyValueWithSchema(Struct value, Schema updatedSchema) {
        Struct updatedValue = new Struct(updatedSchema);
        for (Field field : value.schema().fields()) {
            final Object updatedFieldValue;

            if (value.get(field) == null) { // do not care about the field with no value.
                continue;
            }

            if ((!(config.field_type.length == 0) && !(config.struct_field.length == 0)
                    && containsField(config.struct_field, field.name())
                    && value.get(field) != null)) {
                /**
                 (FieldType) is not empty, which confirms that transformation is to be made to Struct type, which has nested field of columntype.
                 (Nested struct column type change.)
                 */
                if ((field.name().equals(config.struct_field)) || containsField(config.struct_field, field.name())) { //If the struct_field name matches, we parse the nested structure and update with the new value with config.field_type converted timestamp.
                    updatedFieldValue = NestedStructFieldValueUpdate(field, value, updatedSchema.field(field.name()));
                } else { //If not config.struct_field, we leave the field value as it is.
                    updatedFieldValue = value.get(field);
                }

            } else if (field.name().equals(config.field)) {
                updatedFieldValue = convertTimestamp(value.get(field), timestampTypeFromSchema(field.schema()), field);
            } else {
                updatedFieldValue = value.get(field);
            }
            updatedValue.put(field.name(), updatedFieldValue);
        }
        return updatedValue;
    }

    private R applySchemaless(R record) {
        Object value = operatingValue(record);
        return newRecord(record, null, value);

    }

    /**
     * Determine the type/format of the timestamp based on the schema
     */
    private String timestampTypeFromSchema(Schema schema) {
        if (Timestamp.LOGICAL_NAME.equals(schema.name())) {
            return TYPE_TIMESTAMP;
        } else if (org.apache.kafka.connect.data.Date.LOGICAL_NAME.equals(schema.name())) {
            return TYPE_DATE;
        } else if (Time.LOGICAL_NAME.equals(schema.name())) {
            return TYPE_TIME;
        } else if (schema.type().equals(Schema.Type.STRING)) {
            // If not otherwise specified, string == user-specified string format for timestamps
            return TYPE_STRING;
        } else if (schema.type().equals(Schema.Type.INT64)) {
            // If not otherwise specified, long == unix time
            return TYPE_UNIX;
        } else if (schema.type().equals(Schema.Type.INT32)) {
            return TYPE_EPCOH_DATE;
        }
        throw new ConnectException("Schema " + schema + " does not correspond to a known timestamp type format");
    }

    /**
     * Infer the type/format of the timestamp based on the raw Java type
     */
    private String inferTimestampType(Object timestamp) {
        // Note that we can't infer all types, e.g. Date/Time/Timestamp all have the same runtime representation as a
        // java.util.Date
        if (timestamp instanceof Date) {
            return TYPE_TIMESTAMP;
        } else if (timestamp instanceof Long) {
            return TYPE_UNIX;
        } else if (timestamp instanceof String) {
            return TYPE_STRING;
        }
        throw new DataException("TimestampConverter does not support " + timestamp.getClass() + " objects as timestamps");
    }

    /**
     * Convert the given timestamp to the target timestamp format.
     *
     * @param timestamp       the input timestamp
     * @param timestampFormat the format of the timestamp, or null if the format should be inferred
     * @return the converted timestamp
     */
    private Object convertTimestamp(Object timestamp, String timestampFormat, Field originalType) {
        if (timestampFormat == null) {
            timestampFormat = inferTimestampType(timestamp);
        }

        TimestampTranslator sourceTranslator = TRANSLATORS.get(timestampFormat);
        if (sourceTranslator == null) {
            throw new ConnectException("Unsupported timestamp type: " + timestampFormat);
        }
        Date rawTimestamp = sourceTranslator.toRaw(config, timestamp);
        TimestampTranslator targetTranslator;
        if (originalType.name().equals(config.field)) {
            targetTranslator = TRANSLATORS.get(config.type);
        } else {
            targetTranslator = TRANSLATORS.get(TypeConverters.get(originalType.schema().name()));
        }

        if (targetTranslator == null) {
            throw new ConnectException("Unsupported timestamp type: " + config.type);
        }

        if (timestampFormat.equals(TYPE_EPCOH_DATE)) {
            return targetTranslator.toType(config, rawTimestamp, DATE_FORMAT_CONFIG);
        }
        else {
            return targetTranslator.toType(config, rawTimestamp, TIMESTAMP_FORMAT_CONFIG);
        }
    }

    private Object convertTimestamp(Object timestamp) {
        return convertTimestamp(timestamp, null, null);
    }
}
