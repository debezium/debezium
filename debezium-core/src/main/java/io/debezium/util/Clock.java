/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.util.concurrent.TimeUnit;

/**
 * An abstraction for a clock.
 * 
 * @author Randall Hauch
 */
public interface Clock {

    /**
     * The {@link Clock} instance that uses the {@link System} methods.
     */
    static final Clock SYSTEM = new Clock() {
        @Override
        public long currentTimeInMillis() {
            return System.currentTimeMillis();
        }

        @Override
        public long currentTimeInNanos() {
            return System.nanoTime();
        }
    };

    /**
     * Get the {@link Clock} instance that uses the {@link System} methods.
     * @return the system clock; never null
     */
    static Clock system() {
        return SYSTEM;
    }

    /**
     * Get the current time in nanoseconds.
     * @return the current time in nanoseconds.
     */
    default long currentTimeInNanos() {
        return currentTimeInMillis() * 1000000L;
    }
    
    /**
     * Get the current time in microseconds.
     * @return the current time in microseconds.
     */
    default long currentTimeInMicros() {
        return TimeUnit.MICROSECONDS.convert(currentTimeInMillis(), TimeUnit.MILLISECONDS);        
    }

    /**
     * Get the current time in milliseconds.
     * @return the current time in milliseconds.
     */
    public long currentTimeInMillis();

}