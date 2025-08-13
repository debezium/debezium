/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import java.util.List;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * An implementation of {@link AbstractType} for the PostgreSQL {@code tsvector} data type.
 *
 * A {@code tsvector} is a full-text search data type in PostgreSQL. It stores lexeme vectors—
 * preprocessed, searchable representations of textual content—used to support efficient
 * full-text search queries.
 *
 * The PostgreSQL connector serializes {@code tsvector} data using the Debezium semantic type
 * {@link io.debezium.data.TsVector}, which represents the structured content of a tsvector field.
 *
 * Note: Since {@code tsvector} is specific to PostgreSQL and not natively supported by other databases,
 * the JDBC sink connector maps this type to a compatible textual data type in other target systems.
 *
 * The mapping logic (as used in test cases) is as follows:
 * <ul>
 *     <li>PostgreSQL → {@code tsvector}</li>
 *     <li>MySQL → {@code longtext}</li>
 *     <li>SQL Server → {@code varchar}</li>
 *     <li>Oracle → {@code VARCHAR2}</li>
 *     <li>Db2 → {@code CLOB}</li>
 *     <li>Default/others → {@code text}</li>
 * </ul>
 *
 * This approach ensures compatibility across heterogeneous sink systems, enabling full-text
 * content replication even when native support for {@code tsvector} is not available.
 *
 * @author Pranav Tiwari
 */
public class TsvectorType extends AbstractType {

    public static TsvectorType INSTANCE = new TsvectorType();

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "tsvector";
    }

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ io.debezium.data.TsVector.LOGICAL_NAME };
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "cast(? as tsvector)";
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {

        Object finalValue = value == null ? null : ((String) value).replaceAll("'", "");
        return List.of(new ValueBindDescriptor(index, finalValue));
    }
}
