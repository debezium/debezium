/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.converters;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.function.Predicate;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.function.Predicates;
import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import io.debezium.util.Strings;

/**
 * MySQL handles several data types differently between streaming and snapshot and its important
 * that data types be handled consistently across both phases for JDBC sink connectors to create
 * the sink tables properly that adhere to the data provided in both phases.
 *
 * This converter specific makes the following changes:
 *      - {@code BOOLEAN} columns always emitted as INT16 schema types, true=1 and false=0.
 *      - {@code REAL} columns always emitted as FLOAT64 schema types.
 *      - String-based columns always emitted with "__debezium.source.column.character_set" parameter.
 *
 * @author Chris Cranford
 */
public class JdbcSinkDataTypesConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSinkDataTypesConverter.class);

    private static final Short INT16_FALLBACK = (short) 0;
    private static final Float FLOAT32_FALLBACK = 0f;
    private static final Double FLOAT64_FALLBACK = 0d;

    public static final String SELECTOR_BOOLEAN_PROPERTY = "selector.boolean";
    public static final String SELECTOR_REAL_PROPERTY = "selector.real";
    public static final String SELECTOR_STRING_PROPERTY = "selector.string";
    public static final String TREAT_REAL_AS_DOUBLE = "treat.real.as.double";

    private Predicate<RelationalColumn> selectorBoolean = x -> false;
    private Predicate<RelationalColumn> selectorReal = x -> false;
    private Predicate<RelationalColumn> selectorString = x -> false;
    private boolean treatRealAsDouble = true; // MySQL default

    @Override
    public void configure(Properties props) {
        final String booleanSelectorConfig = props.getProperty(SELECTOR_BOOLEAN_PROPERTY);
        if (!Strings.isNullOrBlank(booleanSelectorConfig)) {
            selectorBoolean = Predicates.includes(booleanSelectorConfig.trim(), x -> x.dataCollection() + "." + x.name());
        }
        final String realSelectorConfig = props.getProperty(SELECTOR_REAL_PROPERTY);
        if (!Strings.isNullOrBlank(realSelectorConfig)) {
            selectorReal = Predicates.includes(realSelectorConfig.trim(), x -> x.dataCollection() + "." + x.name());
        }
        final String stringSelectorConfig = props.getProperty(SELECTOR_STRING_PROPERTY);
        if (!Strings.isNullOrBlank(stringSelectorConfig)) {
            selectorString = Predicates.includes(stringSelectorConfig.trim(), x -> x.dataCollection() + "." + x.name());
        }
        final String realAsDouble = props.getProperty(TREAT_REAL_AS_DOUBLE);
        if (!Strings.isNullOrEmpty(realAsDouble)) {
            treatRealAsDouble = Boolean.parseBoolean(realAsDouble);
        }
    }

    @Override
    public void converterFor(RelationalColumn field, ConverterRegistration<SchemaBuilder> registration) {
        if (selectorBoolean.test(field)) {
            registration.register(SchemaBuilder.int16(), getBooleanConverter(field));
        }
        else if (selectorReal.test(field)) {
            if (treatRealAsDouble) {
                registration.register(SchemaBuilder.float64(), getRealConverterDouble(field));
            }
            else {
                registration.register(SchemaBuilder.float32(), getRealConverterFloat(field));
            }
        }
        else if (selectorString.test(field)) {
            final SchemaBuilder schemaBuilder = SchemaBuilder.string();
            schemaBuilder.parameter("__debezium.source.column.character_set", field.charsetName());
            registration.register(schemaBuilder, getStringConverter(field));
        }
    }

    private Converter getBooleanConverter(RelationalColumn field) {
        return value -> {
            if (value == null) {
                if (field.isOptional()) {
                    return null;
                }
                else if (field.hasDefaultValue()) {
                    return toTinyInt((Boolean) field.defaultValue());
                }
                return INT16_FALLBACK;
            }
            else if (value instanceof Boolean) {
                return toTinyInt((Boolean) value);
            }
            else if (value instanceof Number) {
                return toTinyInt(((Number) value).intValue() > 0);
            }
            else if (value instanceof String) {
                try {
                    return toTinyInt(Integer.parseInt((String) value) > 0);
                }
                catch (NumberFormatException e) {
                    return toTinyInt(Boolean.parseBoolean((String) value));
                }
            }
            LOGGER.warn("Cannot convert '{}' to INT16", value.getClass());
            return INT16_FALLBACK;
        };
    }

    private Converter getRealConverterDouble(RelationalColumn field) {
        return value -> {
            if (value == null) {
                if (field.isOptional()) {
                    return null;
                }
                else if (field.hasDefaultValue()) {
                    return (double) field.defaultValue();
                }
                return FLOAT64_FALLBACK;
            }
            else if (value instanceof Number) {
                return ((Number) value).doubleValue();
            }
            else if (value instanceof String) {
                return Double.parseDouble((String) value);
            }
            LOGGER.warn("Cannot convert '{}' to FLOAT64.", value.getClass());
            return FLOAT64_FALLBACK;
        };
    }

    private Converter getRealConverterFloat(RelationalColumn field) {
        return value -> {
            if (value == null) {
                if (field.isOptional()) {
                    return null;
                }
                else if (field.hasDefaultValue()) {
                    return (float) field.defaultValue();
                }
                return FLOAT32_FALLBACK;
            }
            else if (value instanceof Number) {
                return ((Number) value).floatValue();
            }
            else if (value instanceof String) {
                return Float.parseFloat((String) value);
            }
            LOGGER.warn("Cannot convert '{}' to FLOAT32.", value.getClass());
            return FLOAT32_FALLBACK;
        };
    }

    private Converter getStringConverter(RelationalColumn field) {
        return value -> {
            if (value == null) {
                if (field.isOptional()) {
                    return null;
                }
                else if (field.hasDefaultValue()) {
                    return (String) field.defaultValue();
                }
                return "";
            }
            else if (value instanceof byte[]) {
                return new String((byte[]) value, StandardCharsets.UTF_8);
            }
            else if (value instanceof Number) {
                return ((Number) value).toString();
            }
            else if (value instanceof String) {
                return (String) value;
            }
            LOGGER.warn("Cannot convert '{}' to STRING", value.getClass());
            return "";
        };
    }

    private static short toTinyInt(Boolean value) {
        return (short) (value ? 1 : 0);
    }

}