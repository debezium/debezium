/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.infinispan;

import java.util.Arrays;

import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntryImpl;

/**
 * An Infinispan ProtoStream adapter to marshall {@link LogMinerDmlEntryImpl} instances.
 *
 * This class defines a factory for creating {@link LogMinerDmlEntryImpl} instances for when
 * hydrating records from the persisted datastore as well as field handlers to extract instance values
 * to be marshalled to the protocol buffer stream.
 * <p>
 * The underlying protocol buffer record consists of the following structure:
 * <pre>
 *     message LogMinerDmlEntryImpl {
 *         required int32 operation = 1;
 *         string newValues = 2;
 *         string oldValues = 3;
 *         required string name = 4;
 *         required string owner = 5;
 *     }
 * </pre>
 *
 * @author Chris Cranford
 */
@ProtoAdapter(LogMinerDmlEntryImpl.class)
public class LogMinerDmlEntryImplAdapter {

    /**
     * Arrays cannot be serialized with null values and so we use a sentinel value
     * to mark a null element in an array.
     */
    private static final String NULL_VALUE_SENTINEL = "$$DBZ-NULL$$";

    /**
     * A ProtoStream factory that creates a {@link LogMinerDmlEntryImpl} instance from field values.
     *
     * @param operation the operation
     * @param newValues string-array of the after state values
     * @param oldValues string-array of the before state values
     * @param name name of the table
     * @param owner tablespace or schema that owns the table
     * @return the constructed LogMinerDmlEntryImpl instance
     */
    @ProtoFactory
    public LogMinerDmlEntryImpl factory(int operation, String[] newValues, String[] oldValues, String name, String owner) {
        return new LogMinerDmlEntryImpl(operation, stringArrayToObjectArray(newValues), stringArrayToObjectArray(oldValues), owner, name);
    }

    /**
     * A ProtoStream handler to extract the {@code entry} field from the {@link LogMinerDmlEntryImpl}.
     *
     * @param entry the entry instance, must not be {@code null}
     * @return the operation code, never {@code null}
     */
    @ProtoField(number = 1, required = true)
    public int getOperation(LogMinerDmlEntryImpl entry) {
        return entry.getEventType().getValue();
    }

    /**
     * A ProtoStream handler to extract the {@code newValues} object-array from the {@link LogMinerDmlEntryImpl}.
     *
     * @param entry the entry instance, must not be {@code null}
     * @return a string-array of all the after state
     */
    @ProtoField(number = 2)
    public String[] getNewValues(LogMinerDmlEntryImpl entry) {
        // We intentionally serialize the Object[] as a String[] array since strings are registered as a
        // built-in data type for storage into protocol buffers.
        return objectArrayToStringArray(entry.getNewValues());
    }

    /**
     * A ProtoStream handler to extract teh {@code oldValues} object-array from the {@link LogMinerDmlEntryImpl}.
     *
     * @param entry the entry instance, must not be {@code null}
     * @return a string-array of all the before state
     */
    @ProtoField(number = 3)
    public String[] getOldValues(LogMinerDmlEntryImpl entry) {
        // We intentionally serialize the Object[] as a String[] array since strings are registered as a
        // built-in data type for storage into protocol buffers.
        return objectArrayToStringArray(entry.getOldValues());
    }

    /**
     * A ProtoStream handler to extract the {@code objectName} from the {@link LogMinerDmlEntryImpl}.
     *
     * @param entry the entry instance, must not be {@code null}
     * @return the table name
     */
    @ProtoField(number = 4, required = true)
    public String getName(LogMinerDmlEntryImpl entry) {
        return entry.getObjectName();
    }

    /**
     * A ProtoStream handler to extract the {@code objectOwner} from the {@link LogMinerDmlEntryImpl}.
     *
     * @param entry the entry instance, must not be {@code null}
     * @return the tablespace name
     */
    @ProtoField(number = 5, required = true)
    public String getOwner(LogMinerDmlEntryImpl entry) {
        return entry.getObjectOwner();
    }

    /**
     * Converts the provided object-array to a string-array.
     *
     * This conversion is safe since LogMinerDmlParser always populates the object-array with strings.
     * Any element in the array that is {@code null} will be initialized as {@link #NULL_VALUE_SENTINEL}.
     *
     * @param values the values array to be converted, should never be {@code null}
     * @return the values array converted to a string-array
     */
    private String[] objectArrayToStringArray(Object[] values) {
        String[] results = Arrays.copyOf(values, values.length, String[].class);
        for (int i = 0; i < results.length; ++i) {
            if (results[i] == null) {
                results[i] = NULL_VALUE_SENTINEL;
            }
        }
        return results;
    }

    /**
     * Converters the provided string-array to an object-array.
     *
     * This conversion is safe since Strings are also Objects.  This method also is responsible for the
     * conversion of {@link #NULL_VALUE_SENTINEL} sentinel values to {@code null}.
     *
     * @param values the values array to eb converted, should never be {@code null}
     * @return the values array converted to an object-array
     */
    private Object[] stringArrayToObjectArray(String[] values) {
        Object[] results = Arrays.copyOf(values, values.length, Object[].class);
        for (int i = 0; i < results.length; ++i) {
            if (results[i].equals(NULL_VALUE_SENTINEL)) {
                results[i] = null;
            }
        }
        return results;
    }
}
