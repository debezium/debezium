/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.gtid;

import java.util.function.Predicate;

/**
 * Common contract for specifying a Global Transaction Identifier (GTID).
 *
 * @author Chris Cranford
 */
public interface GtidSet {
    /**
     * @return whether the set is empty
     */
    boolean isEmpty();

    /**
     * Checks whether all GTIDs represented by {@code this} set are contained within the supplied set.
     *
     * @param other the other global transaction identifier set; may be null
     * @return true if all global transaction identifiers are present in the provided set, false otherwise
     */
    boolean isContainedWithin(GtidSet other);

    /**
     * Checks whether the provided global transaction identifier is contained within this set.
     *
     * @param gtid the global transaction identifier to locate; may not be null
     * @return true if the set contains the global transaction identifier, false otherwise
     */
    boolean contains(String gtid);

    /**
     * Obtain a copy of the {@link GtidSet}, except with only the GTIDs that are permitted based on the
     * supplied predicate {@code filter}.
     *
     * @param filter the predicate filter to be applied, may be null
     * @return the new global transaction identifier set or {@code this} if the filter is null.
     */
    GtidSet retainAll(Predicate<String> filter);

    /**
     * Subtracts the two global transaction identifier sets.
     *
     * @param other the other set; may be null
     * @return a new {@link GtidSet} instance that contains the difference between the two sets.
     */
    GtidSet subtract(GtidSet other);

    /**
     * Creates a new {@link GtidSet} that contains all global transaction identifiers in this set with those
     * from the supplied other set.
     *
     * @param other the other set with ranges that are added or overwritten on top of those in this set
     * @return the new {@link GtidSet} or {@code this} if the other set is  null or empty; never null
     */
    GtidSet with(GtidSet other);
}
