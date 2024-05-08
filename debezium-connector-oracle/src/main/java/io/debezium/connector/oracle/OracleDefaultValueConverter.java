/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.Immutable;
import io.debezium.annotation.ThreadSafe;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.DefaultValueConverter;
import io.debezium.relational.ValueConverter;
import io.debezium.util.Strings;

import oracle.jdbc.OracleTypes;
import oracle.sql.INTERVALDS;
import oracle.sql.INTERVALYM;
import oracle.sql.TIMESTAMP;
import oracle.sql.TIMESTAMPLTZ;
import oracle.sql.TIMESTAMPTZ;

/**
 * @author Chris Cranford
 */
@ThreadSafe
@Immutable
public class OracleDefaultValueConverter implements DefaultValueConverter {

    private static Logger LOGGER = LoggerFactory.getLogger(OracleDefaultValueConverter.class);

    private final OracleValueConverters valueConverters;
    private final Map<Integer, DefaultValueMapper> defaultValueMappers;

    public OracleDefaultValueConverter(OracleValueConverters valueConverters, OracleConnection jdbcConnection) {
        this.valueConverters = valueConverters;
        this.defaultValueMappers = Collections.unmodifiableMap(createDefaultValueMappers(jdbcConnection));
    }

    @Override
    public Optional<Object> parseDefaultValue(Column column, String defaultValue) {
        final int dataType = column.jdbcType();
        final DefaultValueMapper mapper = defaultValueMappers.get(dataType);
        if (mapper == null) {
            LOGGER.warn("Mapper for type '{}' not found.", dataType);
            return Optional.empty();
        }

        try {
            Object rawDefaultValue = mapper.parse(column, defaultValue != null ? defaultValue.trim() : defaultValue);
            Object convertedDefaultValue = convertDefaultValue(rawDefaultValue, column);
            if (convertedDefaultValue instanceof Struct) {
                // Workaround for KAFKA-12694
                LOGGER.warn("Struct can't be used as default value for column '{}', will use null instead.", column.name());
                return Optional.empty();
            }
            return Optional.ofNullable(convertedDefaultValue);
        }
        catch (Exception e) {
            LOGGER.warn("Cannot parse column default value '{}' to type '{}'.  Expression evaluation is not supported.", defaultValue, dataType);
            LOGGER.debug("Parsing failed due to error", e);
            return Optional.empty();
        }
    }

    private Object convertDefaultValue(Object defaultValue, Column column) {
        // if converters is not null and the default value is not null, we need to convert default value
        if (valueConverters != null && defaultValue != null) {
            final SchemaBuilder schemaBuilder = valueConverters.schemaBuilder(column);
            if (schemaBuilder != null) {
                final Schema schema = schemaBuilder.build();
                // In order to get the valueConverter for this column, we have to create a field;
                // The index value -1 in the field will never used when converting default value;
                // So we can set any number here;
                final Field field = new Field(column.name(), -1, schema);
                final ValueConverter valueConverter = valueConverters.converter(column, field);

                Object result = valueConverter.convert(defaultValue);
                if ((result instanceof BigDecimal) && column.scale().isPresent() && column.scale().get() > ((BigDecimal) result).scale()) {
                    // Note as the scale is increased only, the rounding is more cosmetic.
                    result = ((BigDecimal) result).setScale(column.scale().get(), RoundingMode.HALF_EVEN);
                }
                return result;
            }
        }
        return defaultValue;
    }

    private static Map<Integer, DefaultValueMapper> createDefaultValueMappers(OracleConnection jdbcConnection) {
        // Data types that are supported should be registered in the map. Many of the data types
        // have String-based conversions defined in OracleValueConverters since LogMiner provides
        // column values as strings. The only special handling that is needed here is if a type
        // is formatted with unique characteristics such as single/double quotes for strings.
        //
        // Additionally, we use the OracleTypes numeric representation for data types rather than
        // the type name like we do for SQL Server since the type names can include precision
        // and scale, i.e. TIMESTAMP(6) or INTERVAL YEAR(2) TO MONTH.
        final Map<Integer, DefaultValueMapper> result = new HashMap<>();

        // Numeric types
        result.put(OracleTypes.NUMERIC, nullableDefaultValueMapper());

        // Approximate numerics
        result.put(OracleTypes.BINARY_FLOAT, nullableDefaultValueMapper());
        result.put(OracleTypes.BINARY_DOUBLE, nullableDefaultValueMapper());
        result.put(OracleTypes.FLOAT, nullableDefaultValueMapper());

        // Date and time
        result.put(OracleTypes.DATE, nullableDefaultValueMapper(castTemporalFunctionCall(jdbcConnection)));
        result.put(OracleTypes.TIME, nullableDefaultValueMapper(castTemporalFunctionCall(jdbcConnection)));
        result.put(OracleTypes.TIMESTAMP, nullableDefaultValueMapper(castTemporalFunctionCall(jdbcConnection)));
        result.put(OracleTypes.TIMESTAMPTZ, nullableDefaultValueMapper(castTemporalFunctionCall(jdbcConnection)));
        result.put(OracleTypes.TIMESTAMPLTZ, nullableDefaultValueMapper(castTemporalFunctionCall(jdbcConnection)));
        result.put(OracleTypes.INTERVALYM, nullableDefaultValueMapper(convertIntervalYearMonthStringLiteral()));
        result.put(OracleTypes.INTERVALDS, nullableDefaultValueMapper(convertIntervalDaySecondStringLiteral()));

        // Character strings
        result.put(OracleTypes.CHAR, nullableDefaultValueMapper(enforceCharFieldPadding()));
        result.put(OracleTypes.VARCHAR, nullableDefaultValueMapper(enforceStringUnquote()));

        // Unicode character strings
        result.put(OracleTypes.NCHAR, nullableDefaultValueMapper(enforceCharFieldPadding()));
        result.put(OracleTypes.NVARCHAR, nullableDefaultValueMapper(enforceStringUnquote()));

        // Other data types have been omitted.
        return result;
    }

    private static DefaultValueMapper nullableDefaultValueMapper() {
        return nullableDefaultValueMapper(null);
    }

    private static DefaultValueMapper nullableDefaultValueMapper(DefaultValueMapper mapper) {
        return (column, value) -> {
            if ("NULL".equalsIgnoreCase(value)) {
                return null;
            }
            if (mapper != null) {
                return mapper.parse(column, value);
            }
            return value;
        };
    }

    private static DefaultValueMapper convertIntervalDaySecondStringLiteral() {
        return (column, value) -> {
            if (value != null && value.length() > 2 && value.startsWith("'") && value.endsWith("'")) {
                // When supplied as a string literal, pass to value converter as the Oracle type
                return new INTERVALDS(value.substring(1, value.length() - 1));
            }
            return value;
        };
    }

    private static DefaultValueMapper convertIntervalYearMonthStringLiteral() {
        return (column, value) -> {
            if (value != null && value.length() > 2 && value.startsWith("'") && value.endsWith("'")) {
                // When supplied as a string literal, pass to value converter as the Oracle type
                return new INTERVALYM(value.substring(1, value.length() - 1));
            }
            return value;
        };
    }

    private static DefaultValueMapper enforceCharFieldPadding() {
        return (column, value) -> value != null ? Strings.pad(unquote(value), column.length(), ' ') : null;
    }

    private static DefaultValueMapper enforceStringUnquote() {
        return (column, value) -> value != null ? unquote(value) : null;
    }

    private static DefaultValueMapper castTemporalFunctionCall(OracleConnection jdbcConnection) {
        return (column, value) -> {
            if ("SYSDATE".equalsIgnoreCase(value.trim())) {
                if (column.isOptional()) {
                    // If the column is optional, the default value is ignored
                    return null;
                }
                else if (column.jdbcType() == OracleTypes.TIMESTAMPTZ || column.jdbcType() == OracleTypes.TIMESTAMPLTZ) {
                    // If the column is a TIMESTAMP WITH [LOCAL] TIME ZONE, the non-null default is based on EPOCH
                    return Date.from(Instant.EPOCH);
                }
                else {
                    // For all other temporal types, return "0".
                    // The return is a string-value as the OracleValueConverters know how to explicitly infer
                    // whether to emit the final converted value as either a string or numeric value based on
                    // the column's data type.
                    return "0";
                }
            }
            else if (value.toUpperCase().startsWith("TO_TIMESTAMP")) {
                switch (column.jdbcType()) {
                    case OracleTypes.TIME:
                    case OracleTypes.TIMESTAMP:
                        return JdbcConnection.querySingleValue(jdbcConnection.connection(),
                                "SELECT CAST(" + value + " AS TIMESTAMP) FROM DUAL",
                                st -> {
                                }, rs -> rs.getObject(1, TIMESTAMP.class));
                    case OracleTypes.TIMESTAMPTZ:
                        return JdbcConnection.querySingleValue(jdbcConnection.connection(),
                                "SELECT CAST(" + value + " AS TIMESTAMP WITH TIME ZONE) FROM DUAL",
                                st -> {
                                }, rs -> rs.getObject(1, TIMESTAMPTZ.class));
                    case OracleTypes.TIMESTAMPLTZ:
                        return JdbcConnection.querySingleValue(jdbcConnection.connection(),
                                "SELECT CAST(" + value + " AS TIMESTAMP WITH LOCAL TIME ZONE) FROM DUAL",
                                st -> {
                                }, rs -> rs.getObject(1, TIMESTAMPLTZ.class));
                }
            }
            else if (value.toUpperCase().startsWith("TO_DATE")) {
                if (column.jdbcType() == OracleTypes.DATE || column.jdbcType() == OracleTypes.TIMESTAMP) {
                    return JdbcConnection.querySingleValue(jdbcConnection.connection(),
                            "SELECT CAST(" + value + " AS TIMESTAMP) FROM DUAL",
                            st -> {
                            }, rs -> rs.getObject(1, TIMESTAMP.class));
                }
            }
            return value;
        };
    }

    private static String unquote(String value) {
        if (value.startsWith("('") && value.endsWith("')")) {
            return value.substring(2, value.length() - 2);
        }
        if (value.startsWith("'") && value.endsWith("'")) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }
}
