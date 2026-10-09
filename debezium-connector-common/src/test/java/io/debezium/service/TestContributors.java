/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;

import io.debezium.service.spi.ServiceProviderContributor;

/**
 * Exposes a {@link ServiceProviderContributor} to the {@link java.util.ServiceLoader} only while a test
 * action runs, so that the contributor is not discovered by other tests.
 */
final class TestContributors {

    private TestContributors() {
    }

    /**
     * Runs the action with the contributor discoverable through the thread's context class loader.
     *
     * @param classpathRoot an empty directory that is used as an additional classpath root
     * @param contributor the contributor to expose, must be public with a public no-argument constructor
     * @param action the test action to run
     */
    static void runWith(Path classpathRoot, Class<? extends ServiceProviderContributor> contributor, Runnable action) throws IOException {
        final var services = Files.createDirectories(classpathRoot.resolve("META-INF/services"));
        Files.writeString(services.resolve(ServiceProviderContributor.class.getName()), contributor.getName());

        final var thread = Thread.currentThread();
        final var originalClassLoader = thread.getContextClassLoader();
        try (URLClassLoader classLoader = new URLClassLoader(new URL[]{ classpathRoot.toUri().toURL() }, originalClassLoader)) {
            thread.setContextClassLoader(classLoader);
            action.run();
        }
        finally {
            thread.setContextClassLoader(originalClassLoader);
        }
    }
}
