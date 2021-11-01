/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.postgresql.jdbc.TimestampUtils;
import org.postgresql.util.PGInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.postgresql.PostgresValueConverter;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;

/**
 * Parses and converts column default values.
 */
@ThreadSafe
class PostgresDefaultValueConverter {

    private static Logger LOGGER = LoggerFactory.getLogger(PostgresDefaultValueConverter.class);

    private static final Pattern LITERAL_DEFAULT_PATTERN = Pattern.compile("'(.*?)'");
    private static final Pattern FUNCTION_DEFAULT_PATTERN = Pattern.compile("^[(]?[A-Za-z0-9_]+\\((?:.+(?:, ?.+)*)?\\)");

    /**
     * Converts JDBC string representation of a default column value to an object.
     */
    @FunctionalInterface
    private interface DefaultValueMapper {

        /**
         * Parses string to an object.
         *
         * @param value string representation
         * @return value
         * @throws Exception if there is an parsing error
         */
        Object parse(String value) throws Exception;
    }

    private final PostgresValueConverter valueConverters;
    private final Map<String, DefaultValueMapper> defaultValueMappers;

    PostgresDefaultValueConverter(PostgresValueConverter valueConverters, TimestampUtils timestampUtils) {
        this.valueConverters = valueConverters;
        this.defaultValueMappers = Collections.unmodifiableMap(createDefaultValueMappers(timestampUtils));
    }

    Optional<Object> parseDefaultValue(Column column, String defaultValue) {
        final String dataType = column.typeName();

        final DefaultValueMapper mapper = defaultValueMappers.get(dataType);
        if (mapper == null) {
            LOGGER.warn("Mapper for type '{}' not found.", dataType);
            return Optional.empty();
        }

        try {
            Object rawDefaultValue = mapper.parse(defaultValue);
            Object convertedDefaultValue = convertDefaultValue(rawDefaultValue, column);
            if (convertedDefaultValue == null) {
                return Optional.empty();
            }
            if (convertedDefaultValue instanceof Struct) {
                // Workaround for KAFKA-12694
                LOGGER.warn("Struct can't be used as default value for column '{}', will use null instead.", column.name());
                return Optional.empty();
            }
            return Optional.of(convertedDefaultValue);
        }
        catch (Exception e) {
            LOGGER.warn("Cannot parse column default value '{}' to type '{}'. Expression evaluation is not supported.", defaultValue, dataType);
            LOGGER.debug("Parsing failed due to error", e);
            return Optional.empty();
        }
    }

    private Object convertDefaultValue(Object defaultValue, Column column) {
        // if converters is not null and the default value is not null, we need to convert default value
        if (valueConverters != null && defaultValue != null) {
            final SchemaBuilder schemaBuilder = valueConverters.schemaBuilder(column);
            if (schemaBuilder == null) {
                return defaultValue;
            }
            final Schema schema = schemaBuilder.build();
            // In order to get the valueConverter for this column, we have to create a field;
            // The index value -1 in the field will never used when converting default value;
            // So we can set any number here;
            final Field field = new Field(column.name(), -1, schema);
            final ValueConverter valueConverter = valueConverters.converter(column, field);
            Object result = valueConverter.convert(defaultValue);
            if ((result instanceof BigDecimal) && column.scale().isPresent() && column.scale().get() > ((BigDecimal) result).scale()) {
                // Note that as the scale is increased only, the rounding is more cosmetic.
                result = ((BigDecimal) result).setScale(column.scale().get(), RoundingMode.HALF_EVEN);
            }
            return result;
        }
        return defaultValue;
    }

    private static DefaultValueMapper parseNullDefault(DefaultValueMapper mapper) {
        return x -> {
            if (x.startsWith("NULL::")) {
                return null;
            }
            else {
                return mapper.parse(x);
            }
        };
    }

    private static Map<String, DefaultValueMapper> createDefaultValueMappers(TimestampUtils timestampUtils) {
        final Map<String, DefaultValueMapper> result = new HashMap<>();

        result.put("bit", v -> {
            String defaultValue = extractDefault(v, "00"); // if default is generated, assume length > 1
            if (defaultValue.length() == 1) {
                // treat as a bool
                return "1".equals(defaultValue);
            }
            return defaultValue;
        }); // Sample values: `B'1'::"bit"`, `B'11'::"bit"`
        result.put("varbit", v -> extractDefault(v, "0")); // Sample value: B'110'::"bit"

        result.put("bool", parseNullDefault(v -> Boolean.parseBoolean(extractDefault(v, "false")))); // Sample value: true

        result.put("bpchar", v -> extractDefault(v, "")); // Sample value: 'abcd'::bpchar
        result.put("varchar", v -> extractDefault(v, "")); // Sample value: `abcde'::character varying
        result.put("text", v -> extractDefault(v, "")); // Sample value: 'asdf'::text

        result.put("numeric", parseNullDefault(v -> new BigDecimal(extractDefault(v, "0.0")))); // Sample value: 12345.67891
        result.put("float4", parseNullDefault(v -> Float.parseFloat(extractDefault(v, "0.0")))); // Sample value: 1.234
        result.put("float8", parseNullDefault(v -> Double.parseDouble(extractDefault(v, "0.0")))); // Sample values: `1.234`, `'12345678901234567890'::numeric`
        result.put("int2", parseNullDefault(v -> Short.parseShort(extractDefault(v, "0")))); // Sample value: 32767
        result.put("int4", parseNullDefault(v -> Integer.parseInt(extractDefault(v, "0")))); // Sample value: 123
        result.put("serial", parseNullDefault(v -> Integer.parseInt(extractDefault(v, "0"))));
        result.put("int8", parseNullDefault(v -> Long.parseLong(extractDefault(v, "0")))); // Sample values: `123`, `'9223372036854775807'::bigint`
        result.put("bigserial", parseNullDefault(v -> Long.parseLong(extractDefault(v, "0"))));
        result.put("smallserial", parseNullDefault(v -> Short.parseShort(extractDefault(v, "0"))));

        result.put("json", v -> extractDefault(v, "{}")); // Sample value: '{}'::json
        result.put("jsonb", v -> extractDefault(v, "{}")); // Sample value: '{}'::jsonb
        result.put("xml", v -> extractDefault(v, "")); // Sample value: '<foo>bar</foo>'::xml

        result.put("uuid", v -> UUID.fromString(extractDefault(v, "00000000-0000-0000-0000-000000000000"))); // Sample value: '76019d1a-ad2e-4b22-96e9-1a6d6543c818'::uuid

        result.put("date", v -> timestampUtils.toLocalDateTime(extractDefault(v, "1970-01-01")));
        result.put("time", v -> timestampUtils.toLocalTime(extractDefault(v, "00:00")));
        result.put("timestamp", v -> timestampUtils.toOffsetDateTime(extractDefault(v, "1970-01-01")));
        result.put("timestamptz", v -> timestampUtils.toOffsetDateTime(extractDefault(v, "1970-01-01")));
        result.put("interval", v -> new PGInterval(extractDefault(v, "epoch")));

        // Other data types, such as box, bytea, and more are not handled.
        return result;
    }

    private static String extractDefault(String defaultValue) {
        // Values are either "raw", such as `1234`, or "type casted", such as `'9223372036854775807'::bigint`.
        // If the value does NOT contain a single quote it is assumed to be a raw value. Otherwise the value is
        // extracted from inside the single quotes.
        if (!defaultValue.contains("'")) {
            return defaultValue;
        }

        final Matcher matcher = LITERAL_DEFAULT_PATTERN.matcher(defaultValue);
        matcher.find();
        return matcher.group(1);
    }

    // If the default value is generated by a function, map a placeholder value for the schema
    private static String extractDefault(String defaultValue, String generatedValuePlaceholder) {
        final Matcher functionMatcher = FUNCTION_DEFAULT_PATTERN.matcher(defaultValue);
        if (functionMatcher.find()) {
            return generatedValuePlaceholder;
        }

        return extractDefault(defaultValue);
    }
}
