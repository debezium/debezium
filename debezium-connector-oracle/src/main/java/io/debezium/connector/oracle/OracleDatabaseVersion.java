/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * Represents the Oracle database version.
 *
 * @author Chris Cranford
 */
public class OracleDatabaseVersion {

    private final int major;
    private final int minor;
    private final String banner;

    private OracleDatabaseVersion(int major, int minor, String banner) {
        this.major = major;
        this.minor = minor;
        this.banner = banner;
    }

    public int getMajor() {
        return major;
    }

    public int getMinor() {
        return minor;
    }

    public String getBanner() {
        return banner;
    }

    @Override
    public String toString() {
        return major + "." + minor;
    }

    /**
     * Parse version data from the database driver metadata.
     *
     * @param databaseMetaData the database connection metadata
     * @return the parsed OracleDatabaseVersion
     * @throws SQLException if there was an issue reading the database metadata.
     */
    public static OracleDatabaseVersion parse(DatabaseMetaData databaseMetaData) throws SQLException {
        final int major = databaseMetaData.getDatabaseMajorVersion();
        final int minor = databaseMetaData.getDatabaseMinorVersion();
        final String productVersion = databaseMetaData.getDatabaseProductVersion();

        return new OracleDatabaseVersion(major, minor, productVersion);
    }
}
