/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.util.Arrays;

import io.debezium.annotation.Immutable;

/**
 * Utilities for easily computing hash codes. The algorithm should generally produce good distributions for use in hash-based
 * containers or collections, but as expected does always result in repeatable hash codes given the inputs.
 */
@Immutable
public class HashCode {

    // Prime number used in improving distribution
    private static final int PRIME = 103;

    /**
     * Compute a combined hash code from the supplied objects. This method always returns 0 if no objects are supplied.
     *
     * @param objects the objects that should be used to compute the hash code
     * @return the hash code
     */
    public static int compute(Object... objects) {
        return computeHashCode(0, objects);
    }

    /**
     * Compute a combined hash code from the supplied objects using the supplied seed.
     *
     * @param seed a value upon which the hash code will be based; may be 0
     * @param objects the objects that should be used to compute the hash code
     * @return the hash code
     */
    private static int computeHashCode(int seed,
                                       Object... objects) {
        if (objects == null || objects.length == 0) {
            return seed * HashCode.PRIME;
        }
        // Compute the hash code for all of the objects ...
        int hc = seed;
        for (Object object : objects) {
            hc = HashCode.PRIME * hc;
            if (object instanceof byte[]) {
                hc += Arrays.hashCode((byte[]) object);
            }
            else if (object instanceof boolean[]) {
                hc += Arrays.hashCode((boolean[]) object);
            }
            else if (object instanceof short[]) {
                hc += Arrays.hashCode((short[]) object);
            }
            else if (object instanceof int[]) {
                hc += Arrays.hashCode((int[]) object);
            }
            else if (object instanceof long[]) {
                hc += Arrays.hashCode((long[]) object);
            }
            else if (object instanceof float[]) {
                hc += Arrays.hashCode((float[]) object);
            }
            else if (object instanceof double[]) {
                hc += Arrays.hashCode((double[]) object);
            }
            else if (object instanceof char[]) {
                hc += Arrays.hashCode((char[]) object);
            }
            else if (object instanceof Object[]) {
                hc += Arrays.hashCode((Object[]) object);
            }
            else if (object != null) {
                hc += object.hashCode();
            }
        }
        return hc;
    }

    private HashCode() {
    }

}
