/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

/**
 * Calculates or estimates the size of the object
 *
 * @author Jiri Pechanec
 *
 */
public interface Sizeable {

    long objectSize();
}
