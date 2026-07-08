/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.cockroachdb;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.data.Xml;

/**
 * An implementation of {@link JdbcType} that stores PostgreSQL native types CockroachDB does not
 * provide as {@code text}. The source emits these values as strings, so the textual representation
 * round-trips unchanged. Verified against CockroachDB v25.4 and v26.3: xml, the range types,
 * macaddr, and cidr do not exist there.
 *
 * @author Virag Tripathi
 */
class TextFallbackType extends AbstractType {

    public static final TextFallbackType INSTANCE = new TextFallbackType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Xml.LOGICAL_NAME, "XML", "INT4RANGE", "INT8RANGE", "NUMRANGE", "TSRANGE", "TSTZRANGE", "DATERANGE", "MACADDR", "CIDR" };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "text";
    }

}
