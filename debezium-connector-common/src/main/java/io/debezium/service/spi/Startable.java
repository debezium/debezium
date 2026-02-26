/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service.spi;

import io.debezium.common.annotation.Incubating;

/**
 * A contract that defines that the service wishes to be started during service start-up phase.
 *
 * @author Chris Cranford
 */
@Incubating
public interface Startable {
    /**
     * Request the service to start.
     */
    void start();
}
