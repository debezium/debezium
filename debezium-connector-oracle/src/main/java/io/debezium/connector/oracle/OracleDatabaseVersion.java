/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents the Oracle database version.
 *
 * @author Chris Cranford
 */
public class OracleDatabaseVersion {
    private final static Pattern VERSION_PATTERN = Pattern
            .compile("^([0-9]+)\\.([0-9]+)\\.([0-9]+)\\.([0-9]+)\\.([0-9]+)$");

    private final int major;
    private final int maintenance;
    private final int appServer;
    private final int component;
    private final int platform;
    private final String version;

    private OracleDatabaseVersion(int major, int maintenance, int appServer, int component, int platform, String version) {
        this.major = major;
        this.maintenance = maintenance;
        this.appServer = appServer;
        this.component = component;
        this.platform = platform;
        this.version = version;
    }

    public int getMajor() {
        return major;
    }

    public int getMaintenance() {
        return maintenance;
    }

    public int getAppServer() {
        return appServer;
    }

    public int getComponent() {
        return component;
    }

    public int getPlatform() {
        return platform;
    }

    public String getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return major + "." + maintenance + "." + appServer + "." + component + "." + platform;
    }

    /**
     * Parse the Oracle database version banner.
     *
     * @param version the banner text
     * @return the parsed OracleDatabaseVersion.
     * @throws RuntimeException if the version banner string cannot be parsed
     */
    public static OracleDatabaseVersion parse(String version) {
        Matcher matcher = VERSION_PATTERN.matcher(version);
        if (!matcher.matches()) {
            throw new RuntimeException("Failed to resolve Oracle database version: '" + version + "'");
        }

        int major = Integer.parseInt(matcher.group(1));
        int maintenance = Integer.parseInt(matcher.group(2));
        int app = Integer.parseInt(matcher.group(3));
        int component = Integer.parseInt(matcher.group(4));
        int platform = Integer.parseInt(matcher.group(5));

        return new OracleDatabaseVersion(major, maintenance, app, component, platform, version);
    }
}
