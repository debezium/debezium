/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.converters;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Predicate;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.function.Predicates;
import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import io.debezium.util.Strings;

import oracle.sql.RAW;

/**
 * Oracle emits all {@code RAW} columns as {@code BYTES} by default. There may be use cases where the event
 * should emit the raw data as a {@code STRING} data type instead, and this converter can be used to easily
 * convert the RAW column bytes into a string representation of the data.<p></p>
 *
 * If the RAW column cannot be expressed as string-based data, this converter should not be used.
 *
 * @author Chris Cranford
 */
public class RawToStringConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RawToStringConverter.class);
    private static final String FALLBACK = "";

    public static final String SELECTOR_PROPERTY = "selector";

    private Predicate<RelationalColumn> selector = x -> true;

    @Override
    public void configure(Properties properties) {
        final String selectorConfig = properties.getProperty(SELECTOR_PROPERTY);
        if (Strings.isNullOrEmpty(selectorConfig)) {
            return;
        }
        selector = Predicates.includes(selectorConfig.trim(), x -> x.dataCollection() + "." + x.name());
    }

    @Override
    public void converterFor(RelationalColumn field, ConverterRegistration<SchemaBuilder> registration) {
        if (!"RAW".equalsIgnoreCase(field.typeName()) || !selector.test(field)) {
            return;
        }
        registration.register(SchemaBuilder.string(), x -> {
            if (x == null) {
                if (field.isOptional()) {
                    return null;
                }
                else if (field.hasDefaultValue()) {
                    return field.defaultValue();
                }
                else {
                    return FALLBACK;
                }
            }
            try {
                if (x instanceof String) {
                    String data = (String) x;
                    if (OracleValueConverters.EMPTY_BLOB_FUNCTION.equals(data)) {
                        if (field.isOptional()) {
                            return null;
                        }
                        return FALLBACK;
                    }
                    else if (OracleValueConverters.isHexToRawFunctionCall(data)) {
                        x = RAW.hexString2Bytes(OracleValueConverters.getHexToRawHexString(data));
                    }
                    else {
                        return x;
                    }
                }
                else if (x instanceof RAW) {
                    x = ((RAW) x).getBytes();
                }
                else if (!(x instanceof byte[])) {
                    LOGGER.warn("Cannot convert '{}' to string", x.getClass());
                    return FALLBACK;
                }
                return new String((byte[]) x, StandardCharsets.UTF_8);
            }
            catch (SQLException e) {
                throw new DebeziumException("Failed to convert value for column" + field.name(), e);
            }
        });
    }
}
