/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.relational.Column;
import io.debezium.util.BoundedConcurrentHashMap;
import io.debezium.util.BoundedConcurrentHashMap.Eviction;

/**
 * Implementations return names for fields.
 *
 * @author Chris Cranford
 */
public class FieldNameSelector {

    public static FieldNamer<Column> defaultSelector(boolean sanitizeFieldNames) {
        return sanitizeFieldNames ? new FieldNameCache<>(new FieldNameSanitizer<>(Column::name)) : Column::name;
    }

    public static FieldNamer<String> defaultNonRelationalSelector(boolean sanitizeFieldNames) {
        return sanitizeFieldNames ? new FieldNameCache<>(new FieldNameSanitizer<>((x) -> x)) : (x) -> x;
    }

    /**
     * Implementations determine the field name corresponding to a given column.
     */
    @FunctionalInterface
    public interface FieldNamer<T> {
        String fieldNameFor(T field);
    }

    /**
     * A field namer that replaces any characters invalid in a field with {@code _}.
     */
    private static class FieldNameSanitizer<T> implements FieldNamer<T> {

        private static final Logger LOGGER = LoggerFactory.getLogger(FieldNameSanitizer.class);
        private static final String REPLACEMENT_CHAR = "_";
        private static final String NUMBER_PREFIX = "_";

        private final FieldNamer<T> delegate;

        public FieldNameSanitizer(FieldNamer<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public String fieldNameFor(T column) {
            String fieldName = delegate.fieldNameFor(column);
            return sanitizeColumnName(fieldName);
        }

        /**
         * Sanitize column names that are illegal in Avro
         * Must conform to https://avro.apache.org/docs/1.7.7/spec.html#Names
         *  Legal characters are [a-zA-Z_] for the first character and [a-zA-Z0-9_] thereafter.
         *
         * @param columnName the column name name to be sanitized
         *
         * @return the sanitized name.
         */
        private String sanitizeColumnName(String columnName) {
            boolean changed = false;
            StringBuilder sanitizedNameBuilder = new StringBuilder(columnName.length() + 1);
            for (int i = 0; i < columnName.length(); i++) {
                char c = columnName.charAt(i);
                if (i == 0 && Character.isDigit(c)) {
                    sanitizedNameBuilder.append(NUMBER_PREFIX);
                    sanitizedNameBuilder.append(c);
                    changed = true;
                }
                else if (!(c == '_' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'))) {
                    sanitizedNameBuilder.append(REPLACEMENT_CHAR);
                    changed = true;
                }
                else {
                    sanitizedNameBuilder.append(c);
                }
            }

            final String sanitizedName = sanitizedNameBuilder.toString();
            if (changed) {
                LOGGER.warn("Field '{}' name potentially not safe for serialization, replaced with '{}'", columnName, sanitizedName);
            }

            return sanitizedName;
        }
    }

    /**
     * A field namer that caches names it has obtained from a delegate
     */
    private static class FieldNameCache<T> implements FieldNamer<T> {

        private final BoundedConcurrentHashMap<T, String> fieldNames;
        private final FieldNamer<T> delegate;

        public FieldNameCache(FieldNamer<T> delegate) {
            this.fieldNames = new BoundedConcurrentHashMap<>(10_000, 10, Eviction.LRU);
            this.delegate = delegate;
        }

        @Override
        public String fieldNameFor(T column) {
            return fieldNames.computeIfAbsent(column, delegate::fieldNameFor);
        }
    }
}
