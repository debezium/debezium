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
    public static final Object UNCHANGED_HSTORE_TOAST_VALUE = new Object();

    private static final Object[] UNCHANGED_TOAST_VALUES = {
            UnchangedToastedReplicationMessageColumn.UNCHANGED_TOAST_VALUE,
            UnchangedToastedReplicationMessageColumn.UNCHANGED_HSTORE_TOAST_VALUE };
    private Object unchangedToastValue;

    public UnchangedToastedReplicationMessageColumn(String columnName, PostgresType type, String typeWithModifiers, boolean optional) {
        super(columnName, type, typeWithModifiers, optional);
        setUnchangedToastValue(typeWithModifiers);
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

    private void setUnchangedToastValue(String typeWithModifiers) {
        // Array columns carry their placeholder in the element type, which the value converter resolves from
        // the field schema; the marker below is enough for them.
        unchangedToastValue = "hstore".equals(typeWithModifiers) ? UNCHANGED_HSTORE_TOAST_VALUE : UNCHANGED_TOAST_VALUE;
    }
}
