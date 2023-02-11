/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.debezium.junit.DatabaseVersionResolver.DatabaseVersion;

/**
 * Unit test to verify that the functionality of {@link DatabaseVersionResolver} is accurate.
 *
 * @author Chris Cranford
 */
public class DatabaseVersionResolverTest {

    private DatabaseVersion databaseVersion;

    @Test
    public void testAnnotationLessThanGivenMajorVersion() throws Exception {
        // Database version is same major but with later minor/patch values
        assertThat(checkLessThan(11, 14, 0, 11, -1, -1)).isFalse();
        // Database version is same major but initial minor/patch of 0
        assertThat(checkLessThan(11, 0, 0, 11, -1, -1)).isFalse();
        // Database version is higher major
        assertThat(checkLessThan(12, 0, 0, 11, -1, -1)).isFalse();
        // Database version is lower major
        assertThat(checkLessThan(10, 0, 0, 11, -1, -1)).isTrue();
    }

    @Test
    public void testAnnotationLessThanOrEqualGivenMajorVersion() throws Exception {
        // Database version is same major but with later minor/patch values
        assertThat(checkLessThanEqualTo(11, 14, 0, 11, -1, -1)).isTrue();
        // Database version is same major but initial minor/patch of 0
        assertThat(checkLessThanEqualTo(11, 0, 0, 11, -1, -1)).isTrue();
        // Database version is higher major
        assertThat(checkLessThanEqualTo(12, 0, 0, 11, -1, -1)).isFalse();
        // Database version is lower major
        assertThat(checkLessThanEqualTo(10, 0, 0, 11, -1, -1)).isTrue();
    }

    @Test
    public void testAnnotationEqualToGivenMajorVersion() throws Exception {
        // Database version is same major but with later minor/patch values
        assertThat(checkEqualTo(11, 14, 0, 11, -1, -1)).isTrue();
        // Database version is same major but initial minor/patch of 0
        assertThat(checkEqualTo(11, 0, 0, 11, -1, -1)).isTrue();
        // Database version is higher major
        assertThat(checkEqualTo(12, 0, 0, 11, -1, -1)).isFalse();
        // Database version is lower major
        assertThat(checkEqualTo(10, 0, 0, 11, -1, -1)).isFalse();
    }

    @Test
    public void testAnnotationGreaterThanOrEqualToGivenMajorVersion() throws Exception {
        // Database version is same major but with later minor/patch values
        assertThat(checkGreaterThanOrEqualTo(11, 14, 0, 11, -1, -1)).isTrue();
        // Database version is same major but initial minor/patch of 0
        assertThat(checkGreaterThanOrEqualTo(11, 0, 0, 11, -1, -1)).isTrue();
        // Database version is higher major
        assertThat(checkGreaterThanOrEqualTo(12, 0, 0, 11, -1, -1)).isTrue();
        // Database version is lower major
        assertThat(checkGreaterThanOrEqualTo(10, 0, 0, 11, -1, -1)).isFalse();
    }

    @Test
    public void testAnnotationGreaterThanGivenMajorVersion() throws Exception {
        // Database version is same major but with later minor/patch values
        assertThat(checkGreaterThan(11, 14, 0, 11, -1, -1)).isTrue();
        // Database version is same major but initial minor/patch of 0
        assertThat(checkGreaterThan(11, 0, 0, 11, -1, -1)).isFalse();
        // Database version is higher major
        assertThat(checkGreaterThan(12, 0, 0, 11, -1, -1)).isTrue();
        // Database version is lower major
        assertThat(checkGreaterThan(10, 0, 0, 11, -1, -1)).isFalse();
    }

    private boolean checkLessThan(int dbMajor, int dbMinor, int dbPatch, int major, int minor, int patch) {
        final DatabaseVersion dbVersion = new DatabaseVersion(dbMajor, dbMinor, dbPatch);
        return dbVersion.isLessThan(major, minor, patch);
    }

    private boolean checkLessThanEqualTo(int dbMajor, int dbMinor, int dbPatch, int major, int minor, int patch) {
        final DatabaseVersion dbVersion = new DatabaseVersion(dbMajor, dbMinor, dbPatch);
        return dbVersion.isLessThanEqualTo(major, minor, patch);
    }

    private boolean checkEqualTo(int dbMajor, int dbMinor, int dbPatch, int major, int minor, int patch) {
        final DatabaseVersion dbVersion = new DatabaseVersion(dbMajor, dbMinor, dbPatch);
        return dbVersion.isEqualTo(major, minor, patch);
    }

    private boolean checkGreaterThanOrEqualTo(int dbMajor, int dbMinor, int dbPatch, int major, int minor, int patch) {
        final DatabaseVersion dbVersion = new DatabaseVersion(dbMajor, dbMinor, dbPatch);
        return dbVersion.isGreaterThanEqualTo(major, minor, patch);
    }

    private boolean checkGreaterThan(int dbMajor, int dbMinor, int dbPatch, int major, int minor, int patch) {
        final DatabaseVersion dbVersion = new DatabaseVersion(dbMajor, dbMinor, dbPatch);
        return dbVersion.isGreaterThan(major, minor, patch);
    }
}
