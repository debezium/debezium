/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.debezium.common.annotation.VisibleForTesting;

/**
 * Represents the Oracle database version.
 *
 * @author Chris Cranford
 */
public class OracleDatabaseVersion {
    private final static Pattern VERSION_PATTERN = Pattern
            .compile("(?:.*)(?:Release )([0-9]+)\\.([0-9]+)\\.([0-9]+)\\.([0-9]+)\\.([0-9]+)(?:.*)");
    private final static Pattern VERSION_18_1_PATTERN = Pattern
            .compile("^Oracle Database.*(?:\\r\\n|\\r|\\n)^(?:Version )([0-9]+)\\.([0-9]+)\\.([0-9]+)\\.([0-9]+)\\.([0-9]+)", Pattern.MULTILINE);

    @VisibleForTesting
    public static final OracleDatabaseVersion VERSION_19_3 = new OracleDatabaseVersion(19, 3, 0, 0, 0, "");

    private final int major;
    private final int maintenance;
    private final int appServer;
    private final int component;
    private final int platform;
    private final String banner;

    private OracleDatabaseVersion(int major, int maintenance, int appServer, int component, int platform, String banner) {
        this.major = major;
        this.maintenance = maintenance;
        this.appServer = appServer;
        this.component = component;
        this.platform = platform;
        this.banner = banner;
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

    public String getBanner() {
        return banner;
    }

    @Override
    public String toString() {
        return major + "." + maintenance + "." + appServer + "." + component + "." + platform;
    }

    /**
     * Parse the Oracle database version banner.
     *
     * @param banner the banner text
     * @return the parsed OracleDatabaseVersion.
     * @throws RuntimeException if the version banner string cannot be parsed
     */
    public static OracleDatabaseVersion parse(String banner) {
        Matcher matcher = VERSION_18_1_PATTERN.matcher(banner);
        if (!matcher.matches()) {
            matcher = VERSION_PATTERN.matcher(banner);
            if (!matcher.matches()) {
                throw new RuntimeException("Failed to resolve Oracle database version: '" + banner + "'");
            }
        }

        int major = Integer.parseInt(matcher.group(1));
        int maintenance = Integer.parseInt(matcher.group(2));
        int app = Integer.parseInt(matcher.group(3));
        int component = Integer.parseInt(matcher.group(4));
        int platform = Integer.parseInt(matcher.group(5));

        return new OracleDatabaseVersion(major, maintenance, app, component, platform, banner);
    }
}
