/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import io.debezium.util.OrderedIdBuilder;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * Common information provided by all connectors in either source field or offsets
 *
 * @author Jiri Pechanec
 */
public abstract class AbstractSourceInfo {
    public static final String DEBEZIUM_VERSION_KEY = "version";
    public static final String DEBEZIUM_CONNECTOR_KEY = "connector";
    public static final String ORDER_ID_KEY = "order_id";

    private final String version;
    private final OrderedIdBuilder idBuilder;

    protected static SchemaBuilder schemaBuilder() {
        return SchemaBuilder.struct()
                .field(DEBEZIUM_VERSION_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(DEBEZIUM_CONNECTOR_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(ORDER_ID_KEY, OrderedIdBuilder.schema());
    }

    protected AbstractSourceInfo(String version, OrderedIdBuilder idBuilder) {
        this.version = Objects.requireNonNull(version);
        this.idBuilder = idBuilder;
    }

    /**
     * Returns the schema of specific sub-types. Implementations should call
     * {@link #schemaBuilder()} to add all shared fields to their schema.
     */
    protected abstract Schema schema();

    /**
     * Returns the OrderedIdBuilder for this class
     */
    protected OrderedIdBuilder idBuilder() {
        return idBuilder;
    }

    /**
     * Reset's the idBuilder state to a known value
     * @param state the state to restore
     */
    protected void setIdState(String state) {
        if (state == null) {
            return;
        }
        idBuilder.restoreState(state);
    }

    protected String getNextId() {
        return idBuilder.buildNextId();
    }

    /**
     * Returns the string that identifies the connector relative to the database. Implementations should override
     * this method to specify the connector identifier.
     *
     * @return the connector identifier
     */
    protected abstract String connector();

    protected Struct struct() {
        Struct s = new Struct(schema())
                .put(DEBEZIUM_VERSION_KEY, version)
                .put(DEBEZIUM_CONNECTOR_KEY, connector());
        if (idBuilder.shouldIncludeId()) {
            s.put(ORDER_ID_KEY, idBuilder.lastId());
        }
        return s;
    }

    protected Map<String, Object> buildOffset() {
        HashMap<String, Object> m = new HashMap<>();
        if (idBuilder.shouldIncludeId()) {
            m.put(ORDER_ID_KEY, idBuilder.lastId());
        }
        return m;
    }
}
