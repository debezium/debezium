/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr.client.payloads;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a DDL / schema change event.
 *
 * @author Chris Cranford
 */
public class SchemaChangeEvent extends AbstractPayloadEvent {

    @JsonProperty("schema")
    private PayloadSchema schema;
    private String sql;

    public SchemaChangeEvent() {
        super(Type.DDL);
    }

    public PayloadSchema getSchema() {
        return schema;
    }

    public String getSql() {
        return sql;
    }

    @Override
    public String toString() {
        return "SchemaChangeEvent{" +
                "schema=" + schema +
                ", sql='" + sql + '\'' +
                "} " + super.toString();
    }

}
