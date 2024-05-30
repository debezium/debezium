/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.util;

/**
 * Contract for supplying a unique instance of a test {@link UniqueDatabase}.
 *
 * @author Chris Cranford
 */
public interface UniqueDatabaseProvider {
    /**
     * @return the unique database class to be instantiated.
     */
    Class<? extends UniqueDatabase> getUniqueDatabase();
}
