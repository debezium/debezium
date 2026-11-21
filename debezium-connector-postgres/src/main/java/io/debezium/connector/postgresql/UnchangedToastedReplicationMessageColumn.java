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

    private static final Object[] UNCHANGED_TOAST_VALUES = {
            UnchangedToastedReplicationMessageColumn.UNCHANGED_TOAST_VALUE,
            UnchangedToastedReplicationMessageColumn.UNCHANGED_TEXT_ARRAY_TOAST_VALUE,
            UnchangedToastedReplicationMessageColumn.UNCHANGED_BINARY_ARRAY_TOAST_VALUE,
            UnchangedToastedReplicationMessageColumn.UNCHANGED_INT_ARRAY_TOAST_VALUE,
            UnchangedToastedReplicationMessageColumn.UNCHANGED_BIGINT_ARRAY_TOAST_VALUE,
            UnchangedToastedReplicationMessageColumn.UNCHANGED_HSTORE_TOAST_VALUE,
            UnchangedToastedReplicationMessageColumn.UNCHANGED_UUID_TOAST_VALUE };
    private Object unchangedToastValue;

    public UnchangedToastedReplicationMessageColumn(String columnName, PostgresType type, String typeWithModifiers, boolean optional) {
        super(columnName, type, typeWithModifiers, optional);
        unchangedToastValue = markerForType(typeWithModifiers);
    }

    @Override
    public boolean isToastedColumn() {
        return true;
    }

    public static boolean isUnchangedToastedValue(Object value) {
        // Using Set#contains triggers comparison by the value's hash. For some types, like a Java String, the
        // hash is computed lazily and if the String is a reasonably large value, this can introduce non-negligible
        // overhead costs to compute the hash. Ultimately all we need here is a simple equality check to identify
        // if the supplied value is one of the marker objects.
        for (Object marker : UNCHANGED_TOAST_VALUES) {
            if (marker == value) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Object getValue(PostgresStreamingChangeEventSource.PgConnectionSupplier connection, boolean includeUnknownDatatypes) {
        return unchangedToastValue;
    }

    /**
     * Returns the marker object used for an unchanged toasted column of the given type.
     */
    public static Object markerForType(String typeWithModifiers) {
        typeWithModifiers = removeSizeModifierFromArrayTypes(typeWithModifiers);
        switch (typeWithModifiers) {
            case "text[]":
            case "_text":
            case "character varying[]":
            case "_varchar":
            case "character[]":
            case "_bpchar":
            case "json[]":
            case "_json":
            case "jsonb[]":
            case "_jsonb":
                return UNCHANGED_TEXT_ARRAY_TOAST_VALUE;
            case "bytea[]":
            case "_bytea":
                return UNCHANGED_BINARY_ARRAY_TOAST_VALUE;
            case "integer[]":
            case "_int4":
            case "date[]":
            case "_date":
                return UNCHANGED_INT_ARRAY_TOAST_VALUE;
            case "bigint[]":
            case "_int8":
                return UNCHANGED_BIGINT_ARRAY_TOAST_VALUE;
            case "hstore":
                return UNCHANGED_HSTORE_TOAST_VALUE;
            case "uuid[]":
            case "_uuid":
                return UNCHANGED_UUID_TOAST_VALUE;
            default:
                return UNCHANGED_TOAST_VALUE;
        }
    }

    private static boolean isArrayType(String typeWithModifiers) {
        return typeWithModifiers.startsWith(TYPE_ARRAY_PREFIX) || typeWithModifiers.endsWith(TYPE_ARRAY_SUFFIX);
    }

    protected static String removeSizeModifierFromArrayTypes(String typeWithModifiers) {
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
