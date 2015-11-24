/*
 * Copyright 2015 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

/**
 * @author Randall Hauch
 *
 */
public interface Clock {
    
    public static final Clock SYSTEM = new Clock() {
        @Override
        public long currentTimeInMillis() {
            return System.currentTimeMillis();
        }
        @Override
        public long currentTimeInNanos() {
            return System.nanoTime();
        }
    };
    
    public static Clock system() {
        return SYSTEM;
    }
    
    public long currentTimeInNanos();
    
    public long currentTimeInMillis();

}
