/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
/*
Note: All the null values for any fields should be made to optional, if not while validating the value and schema, we would end up getting null value for field error.
       Going to convert all null value columns into isoptional if they dont contains any data, so to avoid any kinds of (Conversion error: null value for field that is required and has no default value),
       not just for nestedvalues, but also for struct fields.
 */

package io.debezium.transforms.time;

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
import java.time.ZoneId;
import java.util.*;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Background:
 * Debezium currently does not convert the Timestamps/Dates in Mysql DB to formatted String(Timestamp/Date format),
 * but rather produces timestamp values into long values, as Kafka connect supports.
 *
 * This Transformation allows us to change the long values of Timestamp/Date format data into Timestamp/Date formatted String.
 * and we do this passing some additional connector configuration parameters like below,
 *
 * This transform is based on two different set of properties as shown below,
 * 1. "transforms":"TimestampConv"
 *    "transforms.TimestampConv.type":"io.debezium.transforms.time.TimestampConverter$Value"
 *    "transforms.TimestampConv.fields":"<databasename>.<tablename>.<after/before>.<nestedfield>-><destinationtype> || <databasename>.<tablename>.<field>-><destinationtype>"
 *    "transforms.TimestampConv.timestamp.format":"yyyy-MM-dd hh:mm:ss"
 *    "transforms.TimestampConv.date.format":"yyyy-MM-dd"
 *   So, the first option would start converting all the fields or nestedfields provided in the field property.
 *
 * 2. "transforms":"TimestampConv"
 *    "transforms.TimestampConv.type":"io.debezium.transforms.time.TimestampConverter$Value"
 *    "transforms.TimestampConv.field.type":"io.debezium.time.Timestamp->string,io.debezium.time.Date->string"
 *  So, the second option would just convert all the types specified in field.type that are present in Debezium message into its target types, specified in field.type property.
 *
 * 3. Combination of both, in case we would like to convert specific fields into different format, but all others as specified in field.type property.
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Satyajit Vegesna
 */

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * DebeziumTimestampConverter uses this interface to define custom ways of converting different versions/types of timestamp/date formats.
 * DebeziumTimestampConverter has a static block that collects all the custom conversion objects into TRANSLATORS map,
 * and are called based the timestamp type conversion.
 */


public abstract class TimestampConverter<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String FIELD_CONFIG = "fields";
    private static final String FIELD_DEFAULT = "empty";

    public static final String TIMESTAMP_FORMAT_CONFIG = "timestamp.format";
    private static final String TIMESTAMP_FORMAT_DEFAULT = "yyyy-MM-dd HH:mm:ss";

    public static final String DATE_FORMAT_CONFIG = "date.format";
    private static final String DATE_FORMAT_DEFAULT = "yyyy-MM-dd";

    public static final String FIELD_TYPE_CONFIG = "field.type";
    private static final String FIELD_TYPE_DEFAULT = "empty.value";


    private static final ConfigDef FIELD_DOC = new ConfigDef()
            .define(FIELD_CONFIG, ConfigDef.Type.STRING, FIELD_DEFAULT, ConfigDef.Importance.LOW,
                    "The field containing the timestamp,for example: ts_ms is a field, whose entire value is just the long number value and not like a struct. So any field that contains just a value and not struct type value, and in our case we would only have one such field in debezium, ts_ms.")
            .define(TIMESTAMP_FORMAT_CONFIG, ConfigDef.Type.STRING, TIMESTAMP_FORMAT_DEFAULT, ConfigDef.Importance.LOW,
                    "A SimpleDateFormat-compatible format for the timestamp. Used to generate the output when type=string or used to parse the input if the input is a string.")
            .define(DATE_FORMAT_CONFIG, ConfigDef.Type.STRING, DATE_FORMAT_DEFAULT, ConfigDef.Importance.LOW,
                    "A SimpleDateFormat-compatible format for the date. Used to generate the output when type=string or used to parse the input if the input is a string.")
            .define(FIELD_TYPE_CONFIG, ConfigDef.Type.STRING, FIELD_TYPE_DEFAULT, ConfigDef.Importance.LOW, "is a comma-seperated string, providing an option to add multiple converters.\n" +
                    "<Type of Timestamp Object> -> <To Target type>\n" +
                    "can add multiple type converters as below," +
                    " <Type of Timestamp Object> -> <To Target type>,<Type of Timestamp Object> -> <To Target type>");

    private static final String PURPOSE = "converting timestamp formats";
    private static final String TYPE_INT64 = "INT64";
    private static final String TYPE_INT32 = "INT32";
    private static final String TYPE_INT16 = "INT16";
    private static final String TYPE_INT8 = "INT8";
    private static final String TYPE_FLOAT32 = "FLOAT32";
    private static final String TYPE_FLOAT64 = "FLOAT64";
    private static final String TYPE_BOOLEAN = "BOOLEAN";
    private static final String TYPE_STRING_UPPERCASE = "STRING";
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

    private static interface Translator {
        /**
         * Convert from the type-specific format to the universal java.util.Date format
         */
        Date toRaw(TimestampConverter.Config config, Object orig);

        /**
         * Get the schema for this format.
         */
        Schema typeSchema();

        /**
         * Convert from the universal java.util.Date format to the type-specific format
         */
        Object toType(TimestampConverter.Config config, Date orig, String format);

        Schema optionalSchema();
    }


    private static final Map<String, Translator> TRANSLATORS = new ConcurrentHashMap<>();
    private static final Map<String, Schema> OPTIONAL_TRANSLATORS = new ConcurrentHashMap<>();

    static {
        TRANSLATORS.put(TYPE_STRING, new Translator() {
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
            public String toType(Config config, Date orig, String format) {
                synchronized (config.format) {
                    if (format == DATE_FORMAT_CONFIG) {
                        return config.dateFormat.format(orig);
                    } else {
                        return config.format.format(orig);
                    }
                }
            }

            @Override
            public Schema optionalSchema() {
                return Schema.OPTIONAL_STRING_SCHEMA.schema();
            }
        });

        TRANSLATORS.put(TYPE_UNIX, new Translator() {
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
            public Long toType(Config config, Date orig, String format) {
                return Timestamp.fromLogical(Timestamp.SCHEMA, orig);
            }

            @Override
            public Schema optionalSchema() {
                return Schema.OPTIONAL_INT64_SCHEMA.schema();
            }
        });

        TRANSLATORS.put(TYPE_DATE, new Translator() {
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
            public Date toType(Config config, Date orig, String format) {
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

        TRANSLATORS.put(TYPE_TIME, new Translator() {
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
            public Date toType(Config config, Date orig, String format) {
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

        TRANSLATORS.put(TYPE_TIMESTAMP, new Translator() {
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
            public Date toType(Config config, Date orig, String format) {
                return orig;
            }

            @Override
            public Schema optionalSchema() {
                return SchemaBuilder.int64()
                        .name(org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME)
                        .version(1).optional().build().schema();
            }
        });

        TRANSLATORS.put(TYPE_EPCOH_DATE, new Translator() {
            @Override
            public Date toRaw(Config config, Object orig) {
                return Date.from(java.time.LocalDate.ofEpochDay((Integer) orig).atStartOfDay(ZoneId.systemDefault()).toInstant());
            }

            @Override
            public Schema typeSchema() {
                return org.apache.kafka.connect.data.Date.SCHEMA;
            }

            @Override
            public Object toType(Config config, Date orig, String format) {
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


        OPTIONAL_TRANSLATORS.put(TYPE_INT64, Schema.OPTIONAL_INT64_SCHEMA.schema());

        OPTIONAL_TRANSLATORS.put(TYPE_INT32, Schema.OPTIONAL_INT32_SCHEMA.schema());

        OPTIONAL_TRANSLATORS.put(TYPE_INT16, Schema.OPTIONAL_INT16_SCHEMA.schema());

        OPTIONAL_TRANSLATORS.put(TYPE_INT8, Schema.OPTIONAL_INT8_SCHEMA.schema());

        OPTIONAL_TRANSLATORS.put(TYPE_FLOAT64, Schema.OPTIONAL_FLOAT64_SCHEMA.schema());

        OPTIONAL_TRANSLATORS.put(TYPE_FLOAT32, Schema.OPTIONAL_FLOAT32_SCHEMA.schema());

        OPTIONAL_TRANSLATORS.put(TYPE_BOOLEAN, Schema.OPTIONAL_BOOLEAN_SCHEMA.schema());

        OPTIONAL_TRANSLATORS.put(TYPE_STRING_UPPERCASE, Schema.OPTIONAL_STRING_SCHEMA.schema());

        OPTIONAL_TRANSLATORS.put(TYPE_BYTES, Schema.OPTIONAL_BYTES_SCHEMA.schema());

        OPTIONAL_TRANSLATORS.put(TYPE_STRUCT, SchemaBuilder.type(Schema.Type.STRUCT).optional().build().schema());

    }

    /**
     * Static method to check if a particular field is of STRUCT type or not.
     *
     * @return true for struct field and false for others
     */
    protected static Boolean isStruct(Field field) {
        return field.schema().type().getName().equals("struct") ? true : false;
    }


    /**
     * Static method to generate the database.table name from input string.
     *
     * @param schemaName
     * @return database.tablename.
     */

    protected static String databaseTableString(String schemaName) {
        return schemaName.substring(schemaName.indexOf(".") + 1).replace(schemaName.substring(schemaName.lastIndexOf(".")), "");
    }

    /**
     * * Static method to provide a map containing Key(field) and list of Values(nested fields associated to the field and if field is not a struct/nor has nested fields like ts_ms, then entry in map is created a ts_ms -> [None]).
     *
     * @param tableName
     * @return Map containing key as field and value as list of nested field names.
     */
    protected static Map<String, List<String>> getFields(String tableName) {
        return FieldConversions.get(tableName) != null ? FieldConversions.get(tableName).keySet().stream().collect(Collectors.
                groupingBy(key -> key.split("\\.")[0], Collectors.mapping(val -> val.split("\\.").length == 2 ? val.split("\\.")[1].trim() : "None", Collectors.toList())
                )
        ) : Collections.emptyMap();
    }

    /**
     * Static method to sanitize the name of the table from STRUCT schema, example schema.name=SERVERNAME.DATABASENAME.TABLENAME.OTHERS is provided and the method return just the DATABASENAME.TABLENAME.
     *
     * @return DATABASENAME.TABLENAME string.
     */
    protected static Set<String> getTableNames() {
        return FieldConversions.keySet();
    }

    /**
     * Static method to return the fields that are not nested, for example ts_ms , would search based on the None value that we insert using the getfields methods.
     */
    protected static List<String> findFlatFields(String tableName) {
        List<String> flatFieldsArray = new ArrayList<>();
        for (Map.Entry<String, List<String>> flatFields : getFields(tableName).entrySet()) {
            if (flatFields.getValue().size() == 1 && flatFields.getValue().stream().anyMatch(x -> x.equals("None"))) {
                flatFieldsArray.add(flatFields.getKey());
            }
        }
        return flatFieldsArray;
    }

    // This is a bit unusual, but allows the transformation config to be passed to static anonymous classes to customize
    // their behavior
    public static class Config {

        public final String[] field;
        public final SimpleDateFormat format;
        public final SimpleDateFormat dateFormat;
        public final String[] field_type;

        Config(String[] field, SimpleDateFormat format, SimpleDateFormat dateFormat, String[] field_type) {
            this.field = field;
            this.format = format;
            this.dateFormat = dateFormat;
            this.field_type = field_type;
        }
    }

    private Config config;
    private static final Map<String, String> TypeConverters = new HashMap<>();
    private static Map<String, Map<String, String>> FieldConversions = new ConcurrentHashMap<>();

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig simpleConfig = new SimpleConfig(FIELD_DOC, configs);
        final String field[] = simpleConfig.getString(FIELD_CONFIG).split(",");
        final String[] field_type = simpleConfig.getString(FIELD_TYPE_CONFIG).split(",");
        String[] structTypeList = new String[]{};
        String formatPatternTimestamp = simpleConfig.getString(TIMESTAMP_FORMAT_CONFIG);
        String formatPatternDate = simpleConfig.getString(DATE_FORMAT_CONFIG);

        if (!field_type[0].equals(FIELD_TYPE_DEFAULT)) {
            for (String typeMatch : field_type) {
                TypeConverters.put(typeMatch.split("->")[0], typeMatch.split("->")[1]);
            }
            structTypeList = Arrays.copyOf(TypeConverters.values().toArray(), TypeConverters.values().toArray().length, String[].class);
        }

        //Adding elements to FieldConversions, to use them later for finding the right field and change the type accordingly.
        //Two things happening here, 1. Getting FieldConversions added with key(tablename) -> value(List(field name->destination type))
        if (!field[0].equals(FIELD_DEFAULT)) {
            FieldConversions = Arrays.stream(field).collect(Collectors.
                    groupingBy(key -> key.split("\\.")[0] + "." + key.split("\\.")[1].trim(),
                            Collectors.groupingBy(val -> val.replace(val.substring(0, val.indexOf(".", val.indexOf(".") + 1) + 1), "").split("->")[0].trim(),
                                    Collectors.mapping(destTypes -> destTypes.split("->")[1].trim(), Collectors.joining())
                            )
                    ));

            Set<String> destTypes = FieldConversions.values().stream().map(types -> types.values()).flatMap(Collection::stream).collect(Collectors.toSet());
            Set<String> mySet = new HashSet<String>(Arrays.asList(structTypeList));
            mySet.addAll(destTypes);
            structTypeList = Arrays.copyOf(mySet.toArray(), mySet.toArray().length, String[].class);
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
        config = new Config(field, format, dateFormat, field_type);
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
        return FIELD_DOC;
    }

    @Override
    public void close() {
    }

    public static class Key<R extends ConnectRecord<R>> extends TimestampConverter<R> {
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

    public static class Value<R extends ConnectRecord<R>> extends TimestampConverter<R> {
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
    private SchemaBuilder schemaForNestedStruct(Field field, Struct value) {
        SchemaBuilder builder = SchemaUtil.copySchemaBasics(field.schema(), SchemaBuilder.struct());
        for (Field structField : field.schema().fields()) {
            String structFieldSchemaName = structField.schema().name();
            final Object nestedStructField = ((Struct) value.get(field)).get(structField);
            final Boolean fieldCoversionProvided = getFields(databaseTableString(value.schema().name().toString())).get(field.name()) != null ?
                    getFields(databaseTableString(value.schema().name().toString())).get(field.name()).contains(structField.name()) : false;
            //Checks for datatype conversions
            if (structFieldSchemaName != null && !structFieldSchemaName.isEmpty() && TypeConverters.containsKey(structFieldSchemaName) && !fieldCoversionProvided) {
                if (nestedStructField == null || nestedStructField.equals("")) {
                    builder.field(structField.name(), TRANSLATORS.get(TypeConverters.get(structFieldSchemaName)).optionalSchema());
                } else {
                    builder.field(structField.name(), TRANSLATORS.get(TypeConverters.get(structFieldSchemaName)).typeSchema());
                }
            } //Checks for field conversions
            else if (fieldCoversionProvided) {
                if (nestedStructField == null || nestedStructField.equals("")) {
                    builder.field(structField.name(), TRANSLATORS.get(FieldConversions.get(databaseTableString(builder.name().toString())).get(field.name().toString() + "." + structField.name().toString())).optionalSchema());
                } else {
                    builder.field(structField.name(), TRANSLATORS.get(FieldConversions.get(databaseTableString(builder.name().toString())).get(field.name().toString() + "." + structField.name().toString())).typeSchema());
                }
            } else {
                if (nestedStructField == null || nestedStructField.equals("")) {
                    builder.field(structField.name(), OPTIONAL_TRANSLATORS.get(structField.schema().type().toString()));
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
    private Struct valueForNestedStruct(Field field, Struct value, Field nestedUpdatedSchema) {
        Struct updatedNestedStructValue = new Struct(nestedUpdatedSchema.schema());
        String tableName = databaseTableString(value.schema().name().toString());
        for (Field nestedField : ((Struct) value.get(field)).schema().fields()) { //try to convert ((Struct) value.get(field)).get(nestedField) into a single field rather than computing every single time
            final Object updatedNestedFieldValue;
            final Object nestedFieldValue = ((Struct) value.get(field)).get(nestedField);
            final Boolean fieldCoversionProvided = getFields(databaseTableString(value.schema().name().toString())).get(field.name()) != null ?
                    getFields(databaseTableString(value.schema().name().toString())).get(field.name()).contains(nestedField.name()) : false;
            if (nestedFieldValue == null || nestedFieldValue.equals("")) {
                continue;
            }

            if (nestedField.schema().name() != null && !nestedField.schema().name().isEmpty() && TypeConverters.containsKey(nestedField.schema().name()) && (nestedFieldValue != null && !nestedFieldValue.equals("")) && !fieldCoversionProvided) {
                updatedNestedFieldValue = convertTimestamp(nestedFieldValue, timestampTypeFromSchema(nestedField.schema()), nestedField, tableName, field.name() + "." + nestedField.name());
            } else if (fieldCoversionProvided) {
                updatedNestedFieldValue = convertTimestamp(nestedFieldValue, timestampTypeFromSchema(nestedField.schema()), nestedField, tableName, field.name() + "." + nestedField.name());
            } else {
                updatedNestedFieldValue = nestedFieldValue;
            }
            updatedNestedStructValue.put(nestedField.name(), updatedNestedFieldValue);
        }
        return updatedNestedStructValue;
    }

    private R applyWithSchema(R record) {
        final Schema schema = operatingSchema(record);
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
        Schema updatedSchema;
        SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field field : schema.fields()) {
            if (value.get(field) == null || value.get(field).equals("")) {
                builder.field(field.name(), field.schema()).optional();
                continue;
            }

            if (((!(config.field_type.length == 0) && isStruct(field))
                    || getFields(databaseTableString(schema.name().toString())).containsValue(field.name()))
                    && value.get(field) != null) {

                //(FieldType) is not empty, which confirms that transformation is to be made to Struct type, which has nested field of columntype.
                //(Nested struct column type change.)

                SchemaBuilder nestedStructBuilder = schemaForNestedStruct(field, value);
                builder.field(field.name(), nestedStructBuilder.schema());
            }
            // Else condition would convert the non struct type, just based on field_type parameter, as the above if considers only nested or struct conversions
            else if (!(config.field_type.length == 0) && !isStruct(field) && (field.schema().name() != null && !field.schema().name().isEmpty() && TypeConverters.containsKey(field.schema().name()))) {
                builder.field(field.name(), TRANSLATORS.get(TypeConverters.get(field.name().toString())).typeSchema());
            } else if (findFlatFields(databaseTableString(schema.name().toString())).contains(field.name().toString())) {

                //We would have to check for the fields that have None, by calling a method findFlatFields, which basically gives us information about the non nested or non struct column conversion details.--new comment

                builder.field(field.name(), TRANSLATORS.get(FieldConversions.get(databaseTableString(schema.name().toString())).get(field.name())).typeSchema());
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

        Struct updatedValue = applyValueWithSchema(value, updatedSchema);
        return newRecord(record, updatedSchema, updatedValue);
    }

    private Struct applyValueWithSchema(Struct value, Schema updatedSchema) {
        Struct updatedValue = new Struct(updatedSchema);
        String tableName = databaseTableString(value.schema().name().toString());
        for (Field field : value.schema().fields()) {
            final Object updatedFieldValue;

            if (value.get(field) == null) { // do not care about the field with no value.
                continue;
            }

            if (((!(config.field_type.length == 0) && isStruct(field))
                    || getFields(databaseTableString(updatedSchema.name().toString())).containsValue(field.name()))
                    && value.get(field) != null) {
                //Convert any fields in the STRUCT field that are of type provided in the field_type parameters.
                updatedFieldValue = valueForNestedStruct(field, value, updatedSchema.field(field.name()));

            } else if (!(config.field_type.length == 0) && !isStruct(field) && (field.schema().name() != null && !field.schema().name().isEmpty() && TypeConverters.containsKey(field.schema().name()))) {
                updatedFieldValue = convertTimestamp(value.get(field), timestampTypeFromSchema(field.schema()), field, tableName, field.name());
            } else if (findFlatFields(databaseTableString(updatedSchema.name().toString())).contains(field.name().toString())) {
                updatedFieldValue = convertTimestamp(value.get(field), timestampTypeFromSchema(field.schema()), field, tableName, field.name());
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
    private Object convertTimestamp(Object timestamp, String timestampFormat, Field originalType, String tableName, String field) {
        if (timestampFormat == null) {
            timestampFormat = inferTimestampType(timestamp);
        }

        Translator sourceTranslator = TRANSLATORS.get(timestampFormat);
        if (sourceTranslator == null) {
            throw new ConnectException("Unsupported timestamp type: " + timestampFormat);
        }
        Date rawTimestamp = sourceTranslator.toRaw(config, timestamp);
        Translator targetTranslator;

        if (FieldConversions.get(tableName) != null && FieldConversions.get(tableName).containsKey(field)) {
            if (TRANSLATORS.get(FieldConversions.get(tableName).get(field)) == null){
                throw new ConnectException("Unsupported timestamp type: " + FieldConversions.get(tableName).get(field));
            }else {
                targetTranslator = TRANSLATORS.get(FieldConversions.get(tableName).get(field));
            }
        } else {
            if(TRANSLATORS.get(TypeConverters.get(originalType.schema().name()))==null){
                throw new ConnectException("Unsupported timestamp type: " + TypeConverters.get(originalType.schema().name()));
            }
            targetTranslator = TRANSLATORS.get(TypeConverters.get(originalType.schema().name()));
        }

        if (timestampFormat.equals(TYPE_EPCOH_DATE)) {
            return targetTranslator.toType(config, rawTimestamp, DATE_FORMAT_CONFIG);
        } else {
            return targetTranslator.toType(config, rawTimestamp, TIMESTAMP_FORMAT_CONFIG);
        }
    }

    private Object convertTimestamp(Object timestamp) {
        return convertTimestamp(timestamp, null, null, null, null);
    }
}
