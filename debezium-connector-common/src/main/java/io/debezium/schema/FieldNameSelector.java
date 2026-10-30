/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

import io.debezium.relational.Column;
import io.debezium.util.BoundedConcurrentHashMap;
import io.debezium.util.BoundedConcurrentHashMap.Eviction;

/**
 * Implementations return names for fields.
 *
 * @author Chris Cranford
 */
public class FieldNameSelector {

    public static FieldNamer<Column> defaultSelector(SchemaNameAdjuster fieldNameAdjuster) {
        // The field name depends only on the column name, so cache by name rather than by the whole Column.
        // Column keys hash by name only but compare all attributes, so same-named columns of many tables
        // collide and make cache lookups and evictions very expensive.
        final FieldNamer<String> fieldNamesByColumnName = defaultNonRelationalSelector(fieldNameAdjuster);
        return column -> fieldNamesByColumnName.fieldNameFor(column.name());
    }

    public static FieldNamer<String> defaultNonRelationalSelector(SchemaNameAdjuster fieldNameAdjuster) {
        return new FieldNameCache<>(new FieldNameSanitizer<>((x) -> x, fieldNameAdjuster));
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

        private final FieldNamer<T> delegate;
        private final SchemaNameAdjuster fieldNameAdjuster;

        FieldNameSanitizer(FieldNamer<T> delegate, SchemaNameAdjuster fieldNameAdjuster) {
            this.delegate = delegate;
            this.fieldNameAdjuster = fieldNameAdjuster;
        }

        @Override
        public String fieldNameFor(T column) {
            String fieldName = delegate.fieldNameFor(column);
            return fieldNameAdjuster.adjust(fieldName);
        }
    }

    /**
     * A field namer that caches names it has obtained from a delegate
     */
    private static class FieldNameCache<T> implements FieldNamer<T> {

        private final BoundedConcurrentHashMap<T, String> fieldNames;
        private final FieldNamer<T> delegate;

        FieldNameCache(FieldNamer<T> delegate) {
            this.fieldNames = new BoundedConcurrentHashMap<>(10_000, 10, Eviction.LRU);
            this.delegate = delegate;
        }

        @Override
        public String fieldNameFor(T column) {
            return fieldNames.computeIfAbsent(column, delegate::fieldNameFor);
        }
    }
}
