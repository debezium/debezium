/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.mapping;

import java.util.Locale;

import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.util.Strings;

/**
 * A column mapper that adds the {@link #COLUMN_NAME_PARAMETER_KEY}, {@link #TYPE_NAME_PARAMETER_KEY}, {@link #TYPE_LENGTH_PARAMETER_KEY},
 * {@link #TYPE_SCALE_PARAMETER_KEY} and {@link #COLUMN_COMMENT_PARAMETER_KEY} schema parameter keys.
 *
 * @author Orr Ganani
 * @author Gunnar Morling
 */
public class PropagateSourceMetadataToSchemaParameter implements ColumnMapper {

    public static final String TYPE_NAME_PARAMETER_KEY = "__debezium.source.column.type";
    public static final String TYPE_LENGTH_PARAMETER_KEY = "__debezium.source.column.length";
    public static final String TYPE_SCALE_PARAMETER_KEY = "__debezium.source.column.scale";
    public static final String COLUMN_COMMENT_PARAMETER_KEY = "__debezium.source.column.comment";
    public static final String COLUMN_NAME_PARAMETER_KEY = "__debezium.source.column.name";

    @Override
    public ValueConverter create(Column column) {
        return null;
    }

    @Override
    public void alterFieldSchema(Column column, SchemaBuilder schemaBuilder) {
        // upper-casing type names to be consistent across connectors
        schemaBuilder.parameter(TYPE_NAME_PARAMETER_KEY, column.typeName().toUpperCase(Locale.ENGLISH));

        if (column.length() != Column.UNSET_INT_VALUE) {
            schemaBuilder.parameter(TYPE_LENGTH_PARAMETER_KEY, String.valueOf(column.length()));
        }

        if (column.scale().isPresent()) {
            schemaBuilder.parameter(TYPE_SCALE_PARAMETER_KEY, String.valueOf(column.scale().get()));
        }

        if (!Strings.isNullOrEmpty(column.comment())) {
            schemaBuilder.parameter(COLUMN_COMMENT_PARAMETER_KEY, column.comment());
        }

        // set original column name to schema parameters in case the mode "avro" or "avro_unicode"
        // will rename column name from property "field.name.adjustment.mode"
        schemaBuilder.parameter(COLUMN_NAME_PARAMETER_KEY, column.name());
    }
}
