/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.junit;

import java.sql.SQLException;
import java.util.Set;

import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestConnectionService;
import io.debezium.junit.AnnotationBasedTestRule;
import io.debezium.junit.DatabaseVersionResolver;
import io.debezium.junit.EqualityCheck;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.util.Strings;

/**
 * A Junit rule that processes {@link SkipWhenDatabaseIs} annotations and outputs a reason for the skip.
 *
 * @author Chris Cranford
 */
public class SkipTestDependingOnDatabaseRule extends AnnotationBasedTestRule {

    private static final Logger LOGGER = LoggerFactory.getLogger(SkipTestDependingOnDatabaseRule.class);
    private static final boolean IS_MARIADB = resolveMariaDb();
    private static final boolean IS_PERCONA = resolvePercona();

    @Override
    public Statement apply(Statement base, Description description) {
        final SkipWhenDatabaseIsMultiple skipDatabaseIsMultiple = hasAnnotation(description, SkipWhenDatabaseIsMultiple.class);
        if (skipDatabaseIsMultiple != null) {
            for (SkipWhenDatabaseIs skipWhenDatabaseIs : skipDatabaseIsMultiple.value()) {
                final Statement result = applySkipWhenDatabaseIs(skipWhenDatabaseIs, description);
                if (result != null) {
                    return result;
                }
            }
        }
        else {
            final SkipWhenDatabaseIs skipWhenDatabaseIs = hasAnnotation(description, SkipWhenDatabaseIs.class);
            if (skipWhenDatabaseIs != null) {
                final Statement result = applySkipWhenDatabaseIs(skipWhenDatabaseIs, description);
                if (result != null) {
                    return result;
                }
            }
        }
        // Do no skip
        LOGGER.info("No skip performed");
        return base;
    }

    private Statement applySkipWhenDatabaseIs(SkipWhenDatabaseIs skipWhenDatabaseIs, Description description) {
        LOGGER.info("@SkipWhenDatabaseIs detected: " + skipWhenDatabaseIs.value());
        // Check if MariaDB is skipped
        if (IS_MARIADB && skipWhenDatabaseIs.value().equals(SkipWhenDatabaseIs.Type.MARIADB)) {
            final String reason = getDatabaseSkipReason(skipWhenDatabaseIs);
            if (!Strings.isNullOrBlank(reason)) {
                return emptyStatement(reason, description);
            }
        }
        // Check if Percona is skipped
        else if (IS_PERCONA && skipWhenDatabaseIs.value().equals(SkipWhenDatabaseIs.Type.PERCONA)) {
            final String reason = getDatabaseSkipReason(skipWhenDatabaseIs);
            if (!Strings.isNullOrBlank(reason)) {
                return emptyStatement(reason, description);
            }
        }
        // Check if MySQL is skipped
        else if (!IS_MARIADB && !IS_PERCONA && skipWhenDatabaseIs.value().equals(SkipWhenDatabaseIs.Type.MYSQL)) {
            final String reason = getDatabaseSkipReason(skipWhenDatabaseIs);
            if (!Strings.isNullOrBlank(reason)) {
                return emptyStatement(reason, description);
            }
        }
        return null;
    }

    private String getDatabaseSkipReason(SkipWhenDatabaseIs skipWhenDatabaseIs) {
        if (skipWhenDatabaseIs.versions().length == 0) {
            // No versions are specified, use database reason
            return skipWhenDatabaseIs.reason();
        }

        // Only skip based on versions
        SkipWhenDatabaseVersion version = isVersionsSkipped(skipWhenDatabaseIs.versions());
        if (version != null) {
            return version.reason();
        }
        // No version triggered skip, no skip
        return null;
    }

    private SkipWhenDatabaseVersion isVersionsSkipped(SkipWhenDatabaseVersion[] skipWhenDatabaseVersions) {
        for (SkipWhenDatabaseVersion skipWhenDatabaseVersion : skipWhenDatabaseVersions) {
            if (isSkippedByDatabaseVersion(skipWhenDatabaseVersion)) {
                return skipWhenDatabaseVersion;
            }
        }
        return null;
    }

    // todo: taken from SkipTestRule, eventually refactor this logic into a helper class
    private boolean isSkippedByDatabaseVersion(SkipWhenDatabaseVersion skipWhenDatabaseVersion) {

        final EqualityCheck equalityCheck = skipWhenDatabaseVersion.check();
        final int major = skipWhenDatabaseVersion.major();
        final int minor = skipWhenDatabaseVersion.minor();
        final int patch = skipWhenDatabaseVersion.patch();

        // Scans the class path for SkipWhenDatabaseVersionResolver implementations under io.debezium packages
        final Reflections reflections = new Reflections("io.debezium");
        Set<Class<? extends DatabaseVersionResolver>> resolvers = reflections.getSubTypesOf(DatabaseVersionResolver.class);
        Class<? extends DatabaseVersionResolver> resolverClass = resolvers.stream().findFirst().orElse(null);

        if (resolverClass != null) {
            try {
                final DatabaseVersionResolver resolver = resolverClass.getDeclaredConstructor().newInstance();
                DatabaseVersionResolver.DatabaseVersion dbVersion = resolver.getVersion();
                if (dbVersion != null) {
                    switch (equalityCheck) {
                        case LESS_THAN:
                            return dbVersion.isLessThan(major, minor, patch);
                        case LESS_THAN_OR_EQUAL:
                            return dbVersion.isLessThanEqualTo(major, minor, patch);
                        case EQUAL:
                            return dbVersion.isEqualTo(major, minor, patch);
                        case GREATER_THAN_OR_EQUAL:
                            return dbVersion.isGreaterThanEqualTo(major, minor, patch);
                        case GREATER_THAN:
                            return dbVersion.isGreaterThan(major, minor, patch);
                    }
                }
            }
            catch (Exception e) {
                // In the event that the class cannot be loaded, run the test.
                e.printStackTrace();
            }
        }

        return false;
    }

    private static boolean resolveMariaDb() {
        try (BinlogTestConnection db = TestConnectionService.forTestDatabase()) {
            return db.isMariaDb();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean resolvePercona() {
        try (BinlogTestConnection db = TestConnectionService.forTestDatabase()) {
            return db.isPercona();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
