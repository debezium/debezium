/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers.util.dns;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fake DNS resolver which allows tests to work well with testcontainer under Docker Desktop
 *
 * Adaptation of  https://github.com/CorfuDB/CorfuDB/blob/master/it/src/main/java/org/corfudb/universe/universe/docker/FakeDns.java
 */
public class FakeDns {
    public static final Logger LOGGER = LoggerFactory.getLogger(FakeDns.class);

    private static final FakeDns instance = new FakeDns();

    private final Map<String, InetAddress> registry;
    private final AtomicBoolean enabled;

    private FakeDns() {
        this.enabled = new AtomicBoolean(false);
        this.registry = new ConcurrentHashMap<>();
    }

    public static FakeDns getInstance() {
        return instance;
    }

    public void addResolution(String hostname, InetAddress ip) {
        registry.put(hostname, ip);
        LOGGER.info("Added dns resolution: {} -> {}", hostname, ip);
    }

    public void removeResolution(String hostname, InetAddress ip) {
        registry.remove(hostname, ip);
        LOGGER.info("Removed dns resolution: {} -> {}", hostname, ip);
    }

    public Optional<InetAddress> resolve(String hostname) {
        return isEnabled()
                ? resolveInternal(hostname)
                : Optional.empty();
    }

    public Optional<String> resolve(byte[] address) {
        return isEnabled()
                ? resolveInternal(address)
                : Optional.empty();
    }

    private Optional<InetAddress> resolveInternal(String hostname) {
        LOGGER.info("Performing Forward Lookup for HOST : {}", hostname);
        return Optional.of(registry.get(hostname));
    }

    private Optional<String> resolveInternal(byte[] address) {
        LOGGER.info("Performing Reverse Lookup for Address : {}", Arrays.toString(address));
        return registry.keySet()
                .stream()
                .filter(host -> Arrays.equals(registry.get(host).getAddress(), address))
                .findFirst();
    }

    public boolean isEnabled() {
        return enabled.get();
    }

    /**
     * Enable the fake DNS registry
     */
    public FakeDns enable() {
        this.enabled.set(true);
        return this;
    }

    /**
     * Disable the fake DNS registry
     **/
    public FakeDns disable() {
        this.enabled.set(false);
        return this;
    }
}
