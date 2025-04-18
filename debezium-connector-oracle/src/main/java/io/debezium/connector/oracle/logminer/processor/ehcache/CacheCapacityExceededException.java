/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor.ehcache;

import io.debezium.DebeziumException;

/**
 * An exception that identifies that an Ehcache capacity is too small across all configured tiers
 * that the cache engine requested an object to be evicted.
 *
 * @author Chris Cranford
 */
public class CacheCapacityExceededException extends DebeziumException {
    public CacheCapacityExceededException(String cacheName) {
        super(String.format("Cache '%s' capacity exceeded.", cacheName));
    }
}
