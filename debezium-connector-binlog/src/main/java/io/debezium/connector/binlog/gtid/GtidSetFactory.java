/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.gtid;

/**
 * Contract for a factory that creates global transaction identifier sets.
 *
 * @author Chris Cranford
 */
public interface GtidSetFactory {
    /**
     * Constructs a {@link GtidSet} from a string value.
     *
     * @param gtid the global transaction identifier, should not be null
     * @return the constructed global transaction identifier set
     */
    GtidSet createGtidSet(String gtid);
}
