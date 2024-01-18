/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.function.Predicate;

/**
 * Represents a common contract for GTID behavior for MySQL and MariaDB.
 *
 * @author Randall Hauch, Chris Cranford
 */
public interface GtidSet {

    /**
     * Returns whether this {@link GtidSet} is empty.
     */
    boolean isEmpty();

    /**
     * Obtain a copy of this {@link GtidSet} except with only the GTID ranges match the specified predicate.
     *
     * @param sourceFilter the predicate that returns whether a server identifier is to be included
     * @return the new GtidSet, or this object if {@code sourceFilter} is null; never null
     */
    // todo: change to T
    GtidSet retainAll(Predicate<String> sourceFilter);

    /**
     * Determine whether the GTIDs represented by this object are contained completely within the supplied set.
     *
     * @param other the other set of GTIDs; may be null
     * @return {@code true} if all GTIDs are present in the provided set, {@code false} otherwise
     */
    boolean isContainedWithin(GtidSet other);

    /**
     * Obtain a copy of this {@link GtidSet} except overwritten with all the GTID ranges in the supplied {@link GtidSet}.
     *
     * @param other the other {@link GtidSet} with ranges to add/overwrite on top of those in this set
     * @return the new {@link GtidSet}, or this object if {@code other} is null or empty; never null
     */
    GtidSet with(GtidSet other);

    /**
     * Returns a copy of this with all intervals set to the beginning.
     */
    GtidSet getGtidSetBeginning();

    /**
     * Return whether the specified GTID is present in this set.
     *
     * @param gtid the gtid to check; may not be null
     * @return {@code true} if contained by this set, {@code false} otherwise
     */
    boolean contains(String gtid);

    /**
     * Subtracts the two GTID sets.
     *
     * @param other ther other set; may be null
     * @return a new {@link GtidSet} that contains the difference in GTIDs
     */
    GtidSet subtract(GtidSet other);

}
