/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

/**
 * 
 * @author Jiri Pechanec
 *
 */
public interface Nullable {

    /**
     * @return true if this object has real value, false if it is NULL object
     */
    boolean isAvailable();

}