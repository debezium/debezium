/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.ehcache.serialization;

import io.debezium.connector.oracle.OracleValueConverters;

/**
 * @author Chris Cranford
 */
public abstract class AbstractSerializerStream implements AutoCloseable {
    /**
     * Arrays cannot be serialized with null values and so we use a sentinel value
     * to mark a null element in an array.
     */
    protected static final String NULL_VALUE_SENTINEL = "$$DBZ-NULL$$";

    /**
     * The supplied value arrays can now be populated with {@link OracleValueConverters#UNAVAILABLE_VALUE}
     * which is simple java object.  This cannot be represented as a string in the cached Ehcache record
     * and so this sentinel is used to translate the runtime object representation to a serializable form
     * and back during cache to object conversion.
     */
    protected static final String UNAVAILABLE_VALUE_SENTINEL = "$$DBZ-UNAVAILABLE-VALUE$$";
}
