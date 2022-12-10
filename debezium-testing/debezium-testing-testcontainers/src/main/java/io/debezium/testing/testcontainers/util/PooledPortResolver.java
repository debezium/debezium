/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers.util;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves ports from given pool
 */
public class PooledPortResolver implements PortResolver {
    private final Logger LOGGER = LoggerFactory.getLogger(PooledPortResolver.class);

    protected final Set<Integer> ports = new HashSet<>();
    protected final Set<Integer> usedPorts = new HashSet<>();

    /**
     * Creates port resolver backed by given port pool
     *
     * @param ports set of available ports
     */
    public PooledPortResolver(Set<Integer> ports) {
        if (ports == null || ports.isEmpty()) {
            throw new IllegalStateException("Expected a non empty set of ports");
        }

        this.ports.addAll(ports);
    }

    @Override
    public synchronized int resolveFreePort() {
        var port = ports.stream()
                .filter(Predicate.not(usedPorts::contains))
                .findFirst();
        port.ifPresent(usedPorts::add);
        return port.orElseThrow(() -> new IllegalStateException("No free ports remaining"));
    }

    @Override
    public synchronized void releasePort(int port) {
        usedPorts.remove(port);
    }
}
