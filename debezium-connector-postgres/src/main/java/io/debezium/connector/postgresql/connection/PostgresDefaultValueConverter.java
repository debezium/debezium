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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
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

    PostgresDefaultValueConverter(PostgresValueConverter valueConverters) {
        this.valueConverters = valueConverters;
        this.defaultValueMappers = Collections.unmodifiableMap(createDefaultValueMappers());
    }

    Optional<Object> parseDefaultValue(Column column, String defaultValue) {
        final String dataType = column.typeName();

        if (dataType.equals("serial") || dataType.equals("bigserial")) {
            LOGGER.debug("Ignoring db generated default type '{}'", dataType);
            return Optional.empty();
        }

        final DefaultValueMapper mapper = defaultValueMappers.get(dataType);
        if (mapper == null) {
            LOGGER.warn("Mapper for type '{}' not found.", dataType);
            return Optional.empty();
        }

        try {
            Object rawDefaultValue = mapper.parse(defaultValue);
            Object convertedDefaultValue = convertDefaultValue(rawDefaultValue, column);
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

    private Map<String, DefaultValueMapper> createDefaultValueMappers() {
        final Map<String, DefaultValueMapper> result = new HashMap<>();

        result.put("bit", v -> {
            String defaultValue = extractDefault(v);
            if (defaultValue.length() == 1) {
                // treat as a bool
                return "1".equals(defaultValue);
            }
            return defaultValue;
        }); // Sample values: `B'1'::"bit"`, `B'11'::"bit"`
        result.put("varbit", v -> extractDefault(v)); // Sample value: B'110'::"bit"

        result.put("bool", v -> Boolean.parseBoolean(extractDefault(v))); // Sample value: true

        result.put("bpchar", v -> extractDefault(v)); // Sample value: 'abcd'::bpchar
        result.put("varchar", v -> extractDefault(v)); // Sample value: `abcde'::character varying
        result.put("text", v -> extractDefault(v)); // Sample value: 'asdf'::text

        result.put("numeric", v -> new BigDecimal(v)); // Sample value: 12345.67891
        result.put("float4", v -> Float.parseFloat(extractDefault(v))); // Sample value: 1.234
        result.put("float8", v -> Double.parseDouble(extractDefault(v))); // Sample values: `1.234`, `'12345678901234567890'::numeric`
        result.put("int2", v -> Short.parseShort(extractDefault(v))); // Sample value: 32767
        result.put("int4", v -> Integer.parseInt(v)); // Sample value: 123
        result.put("int8", v -> Long.parseLong(extractDefault(v))); // Sample values: `123`, `'9223372036854775807'::bigint`

        result.put("json", v -> extractDefault(v)); // Sample value: '{}'::json
        result.put("jsonb", v -> extractDefault(v)); // Sample value: '{}'::jsonb
        result.put("xml", v -> extractDefault(v)); // Sample value: '<foo>bar</foo>'::xml

        // Other data types, such as box, bytea, date, time and more are not handled.
        return result;
    }

    private String extractDefault(String defaultValue) {
        // Values are either "raw", such as `1234`, or "type casted", such as `'9223372036854775807'::bigint`.
        // If the value does NOT contain a single quote it is assumed to be a raw value. Otherwise the value is
        // extracted from inside the single quotes.
        if (!defaultValue.contains("'")) {
            return defaultValue;
        }

        final Matcher matcher = Pattern.compile("'(.*?)'").matcher(defaultValue);
        matcher.find();
        return matcher.group(1);
    }
}
