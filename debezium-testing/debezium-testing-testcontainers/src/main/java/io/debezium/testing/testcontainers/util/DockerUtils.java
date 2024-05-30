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

public class DockerUtils {

    public static final String CONTAINER_VM_LOG_SKIP = "container.vm.log.skip";
    public static final String CONTAINER_VM_FAKE_DNS = "container.vm.fake.dns";

    /**
     * Checks if system is configured to deploy fake DNS.
     */
    public static boolean isContainerVM() {
        String property = System.getProperty(CONTAINER_VM_FAKE_DNS, "auto");
        return property.equals("true") || (property.equals("auto") && isMac());
    }

    /**
     * Check if running OS is Mac.
     */
    public static boolean isMac() {
        return System.getProperty("os.name").contains("Mac");
    }

    /**
     * If required logs warning banner about required {@code /etc/hosts} entries
     * <br/>
     * The operation is not required and skipped if:
     * <ul>
     *     <li>Containers are not running under Docker Desktop</li>
     *     <li>Skip parameter is {@code true}</li>
     *     <li>{@link #CONTAINER_VM_LOG_SKIP} property is {@code true}</li>
     *     <li>{@link FakeDns} is installed within JVM</li>
     * </ul>
     *
     * @param logger logger used to print the banner
     * @param hosts list of container hostnames
     * @param skip if true the operation is skipped
     */
    public static void logContainerVMBanner(Logger logger, Collection<String> hosts, boolean skip) {
        var prop = System.getProperty(CONTAINER_VM_LOG_SKIP, "false");
        var propertySkip = Boolean.parseBoolean(prop);

        if (propertySkip || skip || !isContainerVM() || FakeDns.getInstance().isInstalled()) {
            return;
        }

        final var newLine = "\n> ";
        final var doubleNewLine = newLine.repeat(2);

        var hostEntries = hosts.stream()
                .map(host -> "127.0.0.1 " + host)
                .collect(Collectors.joining(newLine));

        var banner = new StringBuilder()
                .append("\n>>> VM BASED CONTAINER RUNTIME DETECTED <<<")
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
        if (isContainerVM()) {
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
