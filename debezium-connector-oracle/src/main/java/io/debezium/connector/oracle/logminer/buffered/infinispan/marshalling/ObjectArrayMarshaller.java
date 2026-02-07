/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered.infinispan.marshalling;

import java.util.Arrays;

import io.debezium.connector.oracle.OracleValueConverters;

/**
 * Utility class that helps marshall {@code Object[]} values.
 * <p>
 * LogMiner always provides values in the form of strings until they undergo conversion when the event
 * payload is constructed. So this class can use this to its advantage to serialize the Object[] by
 * treating it as an array of strings.
 *
 * @author Chris Cranford
 */
public class ObjectArrayMarshaller {

    /**
     * Arrays cannot be serialized with null values, and so we use a sentinel value
     * to mark a null element in an array.
     */
    private static final String NULL_VALUE_SENTINEL = "$$DBZ-NULL$$";

    /**
     * The supplied value arrays can now be populated with {@link OracleValueConverters#UNAVAILABLE_VALUE}
     * which is simple java object.  This cannot be represented as a string in the cached Infinispan record
     * and so this sentinel is used to translate the runtime object representation to a serializable form
     * and back during cache to object conversion.
     */
    private static final String UNAVAILABLE_VALUE_SENTINEL = "$$DBZ-UNAVAILABLE-VALUE$$";

    private ObjectArrayMarshaller() {
    }

    /**
     * Converts the provided object-array to a string-array.
     * <p>
     * Internally this method examines the supplied object array and handles conversion for {@literal null}
     * and {@link OracleValueConverters#UNAVAILABLE_VALUE} values so that they can be serialized.
     *
     * @param values the values array to be converted, should never be {@code null}
     * @return the values array converted to a string-array
     */
    public static String[] objectArrayToStringArray(Object[] values) {
        final String[] results = new String[values.length];
        for (int i = 0; i < values.length; ++i) {
            if (values[i] == null) {
                results[i] = NULL_VALUE_SENTINEL;
            }
            else if (values[i] == OracleValueConverters.UNAVAILABLE_VALUE) {
                results[i] = UNAVAILABLE_VALUE_SENTINEL;
            }
            else {
                results[i] = (String) values[i];
            }
        }
        return results;
    }

    /**
     * Converters the provided string-array to an object-array.
     * <p>
     * Internally this method examines the supplied string array and handles the conversion of specific
     * sentinel values back to their runtime equivalents.  For example, {@link #NULL_VALUE_SENTINEL}
     * will be interpreted as {@literal null} and {@link #UNAVAILABLE_VALUE_SENTINEL} will be converted
     * back to {@link OracleValueConverters#UNAVAILABLE_VALUE}.
     *
     * @param values the values array to eb converted, should never be {@code null}
     * @return the values array converted to an object-array
     */
    public static Object[] stringArrayToObjectArray(String[] values) {
        final Object[] results = Arrays.copyOf(values, values.length, Object[].class);
        for (int i = 0; i < results.length; ++i) {
            if (results[i].equals(NULL_VALUE_SENTINEL)) {
                results[i] = null;
            }
            else if (results[i].equals(UNAVAILABLE_VALUE_SENTINEL)) {
                results[i] = OracleValueConverters.UNAVAILABLE_VALUE;
            }
        }
        return results;
    }
}
