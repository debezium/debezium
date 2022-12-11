/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers.util;

import java.net.InetAddress;
import java.util.Collection;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.testcontainers.DockerClientFactory;

public class DockerUtils {

    public static final String DOCKER_DESKTOP_LOG_SKIP_PROPERTY = "docker.desktop.log.skip";
    public static final String DOCKER_DESKTOP_DISABLE_FAKE_DNS = "docker.desktop.disable.fake.dns";

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
     *     <li>{@link FakeDns} is installed within JVM</li>
     * </ul>
     *
     * @param logger logger used to print the banner
     * @param hosts list of container hostnames
     * @param skip if true the operation is skipped
     */
    public static void logDockerDesktopBanner(Logger logger, Collection<String> hosts, boolean skip) {
        var prop = System.getProperty(DOCKER_DESKTOP_LOG_SKIP_PROPERTY, "false");
        var propertySkip = Boolean.parseBoolean(prop);

        if (propertySkip || skip || !isDockerDesktop() || FakeDns.getInstance().isInstalled()) {
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

    /**
     * Enables {@link FakeDns} on docker desktop (if
     */
    public static void enableFakeDnsIfRequired() {
        var property = System.getProperty(DOCKER_DESKTOP_DISABLE_FAKE_DNS, "false");

        if (isDockerDesktop() && !Boolean.parseBoolean(property)) {
            FakeDns.getInstance().install();
        }
    }

    /**
     * Disables {@link FakeDns} if enabled
     */
    public static void disableFakeDns() {
        FakeDns.getInstance().restore();
    }

    /**
     * If {@link FakeDns} is enabled, maps given hostname to {@link InetAddress#getLoopbackAddress()}
     *
     * @param hostname mapped hostname
     */
    public static void addFakeDnsEntry(String hostname) {
        addFakeDnsEntry(hostname, InetAddress.getLoopbackAddress());
    }

    /**
     * If {@link FakeDns} is enabled, maps given hostname to given address
     *
     * @param hostname mapped hostname
     * @param address resolution address
     */
    public static void addFakeDnsEntry(String hostname, InetAddress address) {
        if (FakeDns.getInstance().isInstalled()) {
            FakeDns.getInstance().addResolution(hostname, address);
        }
    }

    /**
     * If {@link FakeDns} is enabled, remove mapping of given hostname to {@link InetAddress#getLoopbackAddress()}
     *
     * @param hostname mapped hostname
     */
    public static void removeFakeDnsEntry(String hostname) {
        removeFakeDnsEntry(hostname, InetAddress.getLoopbackAddress());
    }

    /**
     * If {@link FakeDns} is enabled, remove given mapping
     *
     * @param hostname mapped hostname
     * @param address resolution address
     */
    public static void removeFakeDnsEntry(String hostname, InetAddress address) {
        if (FakeDns.getInstance().isInstalled()) {
            FakeDns.getInstance().removeResolution(hostname, address);
        }
    }

    private DockerUtils() {
        throw new AssertionError("Should not be instantiated");
    }
}
