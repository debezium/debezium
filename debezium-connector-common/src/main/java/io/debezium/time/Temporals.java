/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.time;

import java.time.Duration;

/**
 * Misc. functionality dealing with temporal data types.
 *
 * @author Gunnar Morling
 */
public class Temporals {

    /**
     * Returns that duration from the given ones which represents the larger amount
     * of time ("is longer"). If both durations are equal, that same value will be
     * returned.
     */
    public static Duration max(Duration d1, Duration d2) {
        return d1.compareTo(d2) == 1 ? d1 : d2;
    }

    /**
     * Returns that duration from the given ones which represents the smaller amount
     * of time ("is shorted"). If both durations are equal, that same value will be
     * returned.
     */
    public static Duration min(Duration d1, Duration d2) {
        return d1.compareTo(d2) == 1 ? d2 : d1;
    }
}
