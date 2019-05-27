/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgoutput;

import java.util.List;

import io.debezium.annotation.Immutable;
import io.debezium.relational.TableId;

/**
 * Defines the relational information for a relation-id mapping.
 *
 * @author Chris Cranford
 */
@Immutable
public class PgOutputRelationMetaData {
    private final int relationId;
    private final String schema;
    private final String name;
    private final List<ColumnMetaData> columns;

    /**
     * Construct a pgoutput relation metadata object representing a relational table.
     *
     * @param relationId the postgres relation identifier, unique provided by the pgoutput stream
     * @param schema the schema the table exists within; should never be null
     * @param name the name of the table; should never be null
     * @param columns list of column metadata instances describing the state of each column
     */
    PgOutputRelationMetaData(int relationId, String schema, String name, List<ColumnMetaData> columns) {
        this.relationId = relationId;
        this.schema = schema;
        this.name = name;
        this.columns = columns;
    }

    public int getRelationId() {
        return relationId;
    }

    public String getSchema() {
        return schema;
    }

    public String getName() {
        return name;
    }

    public List<ColumnMetaData> getColumns() {
        return columns;
    }

    public TableId getTableId() {
        return new TableId(null, schema, name);
    }
}
