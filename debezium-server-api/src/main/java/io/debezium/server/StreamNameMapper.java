/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

/**
 * Transforms the name of the record destination to the target stream name.
 *
 * @author Jiri Pechanec
 *
 */
public interface StreamNameMapper {
    String map(String topic);
}
