/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.JdbcType;

/**
 * An implementation of {@link JdbcType} for {@code OID} data types.
 *
 * @author Chris Cranford
 */
class OidType extends AbstractType {

    public static final OidType INSTANCE = new OidType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "OID" };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "oid";
    }

}
