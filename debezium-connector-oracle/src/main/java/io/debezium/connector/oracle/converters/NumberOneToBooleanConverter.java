/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.converters;

import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Predicate;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.function.Predicates;
import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import io.debezium.util.Strings;

import oracle.sql.NUMBER;

/**
 * Oracle reports {@code NUMBER(1)} as a numeric column type by default.  There may be some cases
 * where the consumer would prefer this to be translated to a {@code BOOLEAN} and this converter
 * provides this behavior out of the box.
 *
 * @author Chris Cranford
 */
public class NumberOneToBooleanConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private static final Logger LOGGER = LoggerFactory.getLogger(NumberOneToBooleanConverter.class);
    private static final Boolean FALLBACK = Boolean.FALSE;

    public static final String SELECTOR_PROPERTY = "selector";

    private Predicate<RelationalColumn> selector = x -> true;

    @Override
    public void configure(Properties props) {
        final String selectorConfig = props.getProperty(SELECTOR_PROPERTY);
        if (Strings.isNullOrEmpty(selectorConfig)) {
            return;
        }
        selector = Predicates.includes(selectorConfig.trim(), x -> x.dataCollection() + "." + x.name());
    }

    @Override
    public void converterFor(RelationalColumn field, ConverterRegistration<SchemaBuilder> registration) {
        if (!"NUMBER".equalsIgnoreCase(field.typeName()) || field.length().orElse(-1) != 1 || !selector.test(field)) {
            return;
        }

        registration.register(SchemaBuilder.bool(), x -> {
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
            if (x instanceof Boolean) {
                return x;
            }
            else if (x instanceof Number) {
                return ((Number) x).intValue() > 0;
            }
            else if (x instanceof NUMBER) {
                try {
                    return ((NUMBER) x).intValue() > 0;
                }
                catch (SQLException e) {
                    // ignored, use fallback below
                }
            }
            else if (x instanceof String) {
                try {
                    return Integer.parseInt((String) x) > 0;
                }
                catch (NumberFormatException e) {
                    return Boolean.parseBoolean((String) x);
                }
            }
            LOGGER.warn("Cannot convert '{}' to boolean", x.getClass());
            return FALLBACK;
        });
    }
}
