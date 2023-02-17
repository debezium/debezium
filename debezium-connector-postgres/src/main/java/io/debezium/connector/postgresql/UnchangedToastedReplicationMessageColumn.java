/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import io.debezium.connector.postgresql.connection.AbstractReplicationMessageColumn;

/**
 * Represents a toasted column in a {@link io.debezium.connector.postgresql.connection.ReplicationStream}.
 *
 * Some decoder implementations may stream information about a column but provide an indicator that the field was not
 * changed and therefore toasted.  This implementation acts as an indicator for such fields that are contained within
 * a {@link io.debezium.connector.postgresql.connection.ReplicationMessage}.
 *
 * @author Chris Cranford
 */
public class UnchangedToastedReplicationMessageColumn extends AbstractReplicationMessageColumn {

    /**
     * Marker value indicating an unchanged TOAST column value.
     */
    public static final Object UNCHANGED_TOAST_VALUE = new Object();
    public static final Object UNCHANGED_TEXT_ARRAY_TOAST_VALUE = new Object();
    public static final Object UNCHANGED_INT_ARRAY_TOAST_VALUE = new Object();
    public static final Object UNCHANGED_BIGINT_ARRAY_TOAST_VALUE = new Object();
    public static final Object UNCHANGED_HSTORE_TOAST_VALUE = new Object();

    private Object unchangedToastValue;

    public UnchangedToastedReplicationMessageColumn(String columnName, PostgresType type, String typeWithModifiers, boolean optional) {
        super(columnName, type, typeWithModifiers, optional);
        setUnchangedToastValue(typeWithModifiers);
    }

    @Override
    public boolean isToastedColumn() {
        return true;
    }

    @Override
    public Object getValue(PostgresStreamingChangeEventSource.PgConnectionSupplier connection, boolean includeUnknownDatatypes) {
        return unchangedToastValue;
    }

    private void setUnchangedToastValue(String typeWithModifiers) {
        // Removing the size for type like _varchar(2000, 0)
        int parenthesis = typeWithModifiers.indexOf("(");
        if (parenthesis > 0) {
            typeWithModifiers = typeWithModifiers.substring(0, parenthesis);
        }
        switch (typeWithModifiers) {
            case "text[]":
            case "_text":
            case "character varying[]":
            case "_varchar":
            case "json[]":
            case "_json":
            case "jsonb[]":
            case "_jsonb":
                unchangedToastValue = UNCHANGED_TEXT_ARRAY_TOAST_VALUE;
                break;
            case "integer[]":
            case "_int4":
            case "_date":
                unchangedToastValue = UNCHANGED_INT_ARRAY_TOAST_VALUE;
                break;
            case "bigint[]":
            case "_int8":
                unchangedToastValue = UNCHANGED_BIGINT_ARRAY_TOAST_VALUE;
                break;
            case "hstore":
                unchangedToastValue = UNCHANGED_HSTORE_TOAST_VALUE;
                break;
            default:
                unchangedToastValue = UNCHANGED_TOAST_VALUE;
        }
    }
}
