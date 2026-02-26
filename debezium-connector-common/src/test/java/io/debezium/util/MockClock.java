/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

/**
 * @author Randall Hauch
 *
 */
public class MockClock implements Clock {

    private long currentTimeInMillis;

    public MockClock() {
    }

    public MockClock(long timeInMillis) {
        assert timeInMillis >= 0;
        currentTimeInMillis = timeInMillis;
    }

    public MockClock advanceTo(long timeInMillis) {
        assert timeInMillis >= 0;
        currentTimeInMillis = timeInMillis;
        return this;
    }

    public MockClock increment(long timeInMillis) {
        assert timeInMillis >= 0;
        currentTimeInMillis += timeInMillis;
        return this;
    }

    @Override
    public long currentTimeInMillis() {
        return currentTimeInMillis;
    }

    @Override
    public String toString() {
        return Strings.duration(currentTimeInMillis);
    }
}
