/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers.util;

import java.util.Collection;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.testcontainers.DockerClientFactory;

public class DockerUtils {

    public static final String DOCKER_DESKTOP_LOG_SKIP_PROPERTY = "docker.desktop.log.skip";

    public static boolean isDockerDesktop() {
        var info = DockerClientFactory.instance().getInfo();
        return "docker-desktop".equals(info.getName());
    }

    /**
     * If required logs warning banner about required {@code /etc/hosts} entries
     * <br/>
     * The operation is not required and skipped if:
     * <ul>
     *     <li>Containers are not running under Docker Desktop</li>
     *     <li>Skip parameter is {@code true}</li>
     *     <li>{@link #DOCKER_DESKTOP_LOG_SKIP_PROPERTY} property is {@code true}</li>
     * </ul>
     *
     * @param logger logger used to print the banner
     * @param hosts list of container hostnames
     * @param skip if true the operation is skipped
     */
    public static void logDockerDesktopBanner(Logger logger, Collection<String> hosts, boolean skip) {
        var prop = System.getProperty(DOCKER_DESKTOP_LOG_SKIP_PROPERTY, "false");
        var propertySkip = Boolean.parseBoolean(prop);

        if (propertySkip || skip || !isDockerDesktop()) {
            return;
        }

        final var newLine = "\n> ";
        final var doubleNewLine = newLine.repeat(2);

        var hostEntries = hosts.stream()
                .map(host -> "127.0.0.1 " + host)
                .collect(Collectors.joining(newLine));

        var banner = new StringBuilder()
                .append("\n>>> DOCKER DESKTOP DETECTED <<<")
                .append(doubleNewLine)
                .append("Requires the following entries in /etc/hosts or equivalent of your platform")
                .append(doubleNewLine)
                .append(hostEntries)
                .append(newLine)
                .append("\n>>> Missing entries will likely lead to failures");

        logger.warn(banner.toString());
    }

    private DockerUtils() {
        throw new AssertionError("Should not be instantiated");
    }
}
