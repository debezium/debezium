/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spi.common;

import java.util.Map;

/**
 * A contract that defines a configurable interface
 *
 * @author Mario Fiore Vitale
 *
 */
public interface Configurable {

    /**
     * Connector properties are passed to let you configure your implementation.
     *
     * @param properties map of configurable properties
     */
    void configure(Map<String, ?> properties);
}
