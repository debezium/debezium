/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.sqlserver;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.relational.ColumnDescriptor;
import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.data.Xml;

/**
 * An implementation of {@link Type} for {@code XML} data types.
 *
 * @author Chris Cranford
 */
class XmlType extends AbstractType {

    public static final XmlType INSTANCE = new XmlType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Xml.LOGICAL_NAME, "XML" };
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "cast(? as xml)";
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        return "xml";
    }

}
