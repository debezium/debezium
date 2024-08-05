/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.ehcache.serialization;

import java.util.Arrays;

import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;

/**
 * A utility class for serializing/deserializing a {@link LogMinerDmlEntry} object.
 *
 * @author Chris Cranford
 */
public class DmlEntrySerdes {

    /**
     * Arrays cannot be serialized with null values and so we use a sentinel value
     * to mark a null element in an array.
     */
    public static final String NULL_VALUE_SENTINEL = "$$DBZ-NULL$$";

    /**
     * The supplied value arrays can now be populated with {@link OracleValueConverters#UNAVAILABLE_VALUE}
     * which is simple java object.  This cannot be represented as a string in the cached Infinispan record
     * and so this sentinel is used to translate the runtime object representation to a serializable form
     * and back during cache to object conversion.
     */
    public static final String UNAVAILABLE_VALUE_SENTINEL = "$$DBZ-UNAVAILABLE-VALUE$$";

    /**
     * Convert an array of object values to a string array for serialization. This is typically
     * used when preparing the {@link LogMinerDmlEntry} values array for serialization.
     *
     * @param values the dml entry's new or old object values array
     * @return string array of values prepared for serialization
     */
    public static String[] objectArrayToStringArray(Object[] values) {
        String[] results = new String[values.length];
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
     * Convert an array of string values, typically from the encoded stream back into their
     * Java object representations after being deserialized.
     *
     * @param values array of string values to convert
     * @return array of objects after conversion
     */
    public static Object[] stringArrayToObjectArray(String[] values) {
        Object[] results = Arrays.copyOf(values, values.length, Object[].class);
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

    private DmlEntrySerdes() {
    }
}
