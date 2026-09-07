/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.connect;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.connector.jdbc.util.StructToJsonConverter;

/**
 * An implementation of {@link JdbcType} for {@code STRUCT} schema types that no dedicated,
 * schema-named handler is registered for. This is created as an abstract implementation as it is
 * expected that each dialect will create its own implementation as the logic to handle struct-based
 * schema types differs by dialect.
 */
public abstract class AbstractConnectStructType extends AbstractConnectSchemaType {

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "STRUCT" };
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        // No default value is permitted
        return null;
    }

    protected String structToJsonString(Struct struct) {
        return StructToJsonConverter.structToJsonString(struct);
    }

}
