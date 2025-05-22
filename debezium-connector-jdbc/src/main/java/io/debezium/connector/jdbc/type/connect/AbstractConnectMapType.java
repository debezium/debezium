/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.connect;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.jdbc.type.JdbcType;

/**
 * An implementation of {@link JdbcType} for {@code MAP} schema types. This is created an abstract
 * implementation as its expected that each dialect will create its own implementation as the
 * logic to handle map-based schema types differs by dialect.
 *
 * @author Chris Cranford
 */
public abstract class AbstractConnectMapType extends AbstractConnectSchemaType {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "MAP" };
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        // No default value is permitted
        return null;
    }

    protected String mapToJsonString(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        }
        catch (JsonProcessingException e) {
            throw new ConnectException("Failed to deserialize MAP data to JSON", e);
        }
    }

}
