/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr.client.payloads;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.debezium.relational.TableId;

/**
 * Represents a schema block associated with various payload events.
 *
 * @author Chris Cranford
 */
public class PayloadSchema {

    private String owner;
    private String table;
    @JsonProperty("obj")
    private Long objectId;
    private List<SchemaColumn> columns;
    @JsonIgnore
    private TableId tableId;

    public String getOwner() {
        return owner;
    }

    public String getTable() {
        return table;
    }

    public Long getObjectId() {
        return objectId;
    }

    public List<SchemaColumn> getColumns() {
        return columns;
    }

    // todo: currently OpenLogReplicator does not expose the pluggable or root database name, pass configured value
    public TableId getTableId(String catalogName) {
        if (tableId == null) {
            tableId = new TableId(catalogName, owner, table);
        }
        return tableId;
    }

    @Override
    public String toString() {
        return "PayloadSchema{" +
                "owner='" + owner + '\'' +
                ", table='" + table + '\'' +
                ", objectId=" + objectId +
                ", columns=" + columns +
                '}';
    }

}
