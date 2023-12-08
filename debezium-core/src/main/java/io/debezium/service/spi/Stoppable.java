/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service.spi;

import io.debezium.common.annotation.Incubating;

/**
 * A contract that defines that the service wishes to be stopped during service shutdown.
 * This is useful if the service has some resources that should be closed.
 *
 * @author Chris Cranford
 */
@Incubating
public interface Stoppable {
    /**
     * Request the service to stop.
     */
    void stop();
}
