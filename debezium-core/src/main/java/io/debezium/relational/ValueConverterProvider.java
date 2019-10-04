/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * A provider of {@link ValueConverter} functions and the {@link SchemaBuilder} used to describe them.
 *
 * @author Randall Hauch
 */
public interface ValueConverterProvider {

    /**
     * Returns a {@link SchemaBuilder} for a {@link Schema} describing literal values of the given JDBC type.
     *
     * @param columnDefinition the column definition; never null
     * @return the schema builder; null if the column's type information is unknown
     */
    public SchemaBuilder schemaBuilder(Column columnDefinition);

    /**
     * Returns a {@link ValueConverter} that can be used to convert the JDBC values corresponding to the given JDBC temporal type
     * into literal values described by the {@link #schemaBuilder(Column) schema}.
     * <p>
     * This method is only called when {@link #schemaBuilder(Column)} returns a non-null {@link SchemaBuilder} for the same column
     * definition.
     *
     * @param columnDefinition the column definition; never null
     * @param fieldDefn the definition for the field in a Kafka Connect {@link Schema} describing the output of the function;
     *            never null
     * @return the value converter; never null
     */
    public ValueConverter converter(Column columnDefinition, Field fieldDefn);
}
