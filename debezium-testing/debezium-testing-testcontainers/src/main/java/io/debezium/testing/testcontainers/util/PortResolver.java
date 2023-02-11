/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers.util;

/**
 *  Port resolver
 */
public interface PortResolver {

    /**
     * Resolves a free port
     *
     * @throws IllegalStateException when no port can be resolved
     * @return free port
     */
    int resolveFreePort();

    void releasePort(int port);
}
