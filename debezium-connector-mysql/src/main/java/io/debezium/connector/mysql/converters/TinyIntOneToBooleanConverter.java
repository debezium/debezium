/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.converters;

import java.util.List;
import java.util.Properties;
import java.util.function.Predicate;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.function.Predicates;
import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import io.debezium.util.Collect;
import io.debezium.util.Strings;

/**
 * MySQL reports {@code BOOLEAN} values as {@code TINYINT(1)} in snapshot phase even as a result of
 * {@code DESCRIBE CREATE TABLE}.
 * This custom converter allows user to handle all {@code TINYINT(1)} fields as {@code BOOLEAN} or provide
 * a set of regexes to match only subset of tables/columns.
 *
 * @author Jiri Pechanec
 */
public class TinyIntOneToBooleanConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private static final Boolean FALLBACK = Boolean.FALSE;

    public static final String SELECTOR_PROPERTY = "selector";
    // recommend disabling this option for mysql8 since "show create table" not showing length of tinyint unsigned type,
    // and specify the columns that need to be converted to "selector" property instead of converting all columns based on type.
    public static final String LENGTH_CHECKER = "length.checker";

    private static final List<String> TINYINT_FAMILY = Collect.arrayListOf("TINYINT", "TINYINT UNSIGNED");

    private static final Logger LOGGER = LoggerFactory.getLogger(TinyIntOneToBooleanConverter.class);

    private Predicate<RelationalColumn> selector = x -> true;
    private boolean lengthChecker = true;

    @Override
    public void configure(Properties props) {
        final String selectorConfig = props.getProperty(SELECTOR_PROPERTY);
        if (!Strings.isNullOrEmpty(selectorConfig)) {
            selector = Predicates.includes(selectorConfig.trim(), x -> x.dataCollection() + "." + x.name());
        }
        final String lengthCheckerConfig = props.getProperty(LENGTH_CHECKER);
        if (!Strings.isNullOrEmpty(lengthCheckerConfig)) {
            lengthChecker = Boolean.parseBoolean(lengthCheckerConfig);
        }
    }

    @Override
    public void converterFor(RelationalColumn field, ConverterRegistration<SchemaBuilder> registration) {
        if (!TINYINT_FAMILY.contains(field.typeName().toUpperCase())
                || (lengthChecker && field.length().orElse(-1) != 1) || !selector.test(field)) {
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
                return FALLBACK;
            }
            if (x instanceof Boolean) {
                return x;
            }
            else if (x instanceof Number) {
                return ((Number) x).intValue() > 0;
            }
            else if (x instanceof String) {
                try {
                    return Integer.parseInt((String) x);
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
