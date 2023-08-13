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

    private static final String TYPE_ARRAY_SUFFIX = "[]";
    private static final String TYPE_ARRAY_PREFIX = "_";
    /**
     * Marker value indicating an unchanged TOAST column value.
     */
    public static final Object UNCHANGED_TOAST_VALUE = new Object();
    public static final Object UNCHANGED_TEXT_ARRAY_TOAST_VALUE = new Object();
    public static final Object UNCHANGED_BINARY_ARRAY_TOAST_VALUE = new Object();
    public static final Object UNCHANGED_INT_ARRAY_TOAST_VALUE = new Object();
    public static final Object UNCHANGED_BIGINT_ARRAY_TOAST_VALUE = new Object();
    public static final Object UNCHANGED_HSTORE_TOAST_VALUE = new Object();
    public static final Object UNCHANGED_UUID_TOAST_VALUE = new Object();

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
        typeWithModifiers = removeSizeModifierFromArrayTypes(typeWithModifiers);
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
            case "bytea[]":
            case "_bytea":
                unchangedToastValue = UNCHANGED_BINARY_ARRAY_TOAST_VALUE;
                break;
            case "integer[]":
            case "_int4":
            case "date[]":
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
            case "uuid[]":
            case "_uuid":
                unchangedToastValue = UNCHANGED_UUID_TOAST_VALUE;
                break;
            default:
                unchangedToastValue = UNCHANGED_TOAST_VALUE;
        }
    }

    private boolean isArrayType(String typeWithModifiers) {
        return typeWithModifiers.startsWith(TYPE_ARRAY_PREFIX) || typeWithModifiers.endsWith(TYPE_ARRAY_SUFFIX);
    }

    protected String removeSizeModifierFromArrayTypes(String typeWithModifiers) {
        // Removing the size for type like _varchar(2000, 0)
        if (isArrayType(typeWithModifiers)) {
            final int leftParenthesis = typeWithModifiers.indexOf("(");
            final int rightParenthesis = typeWithModifiers.lastIndexOf(")");
            if (leftParenthesis > 0 && rightParenthesis > 0) {
                if (rightParenthesis == typeWithModifiers.length() - 1) {
                    typeWithModifiers = typeWithModifiers.substring(0, leftParenthesis);
                }
                else {
                    typeWithModifiers = typeWithModifiers.substring(0, leftParenthesis)
                            + typeWithModifiers.substring(rightParenthesis + 1);
                }
            }
        }
        return typeWithModifiers;
    }
}
