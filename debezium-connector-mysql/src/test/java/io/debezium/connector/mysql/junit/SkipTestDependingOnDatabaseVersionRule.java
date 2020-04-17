/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.junit;

import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import io.debezium.connector.mysql.MySQLConnection;
import io.debezium.connector.mysql.MySQLConnection.MySqlVersion;
import io.debezium.junit.AnnotationBasedTestRule;

/**
 * JUnit rule that skips a test based on the {@link SkipWhenDatabaseVersion} annotation either on a test method or
 * on a test class.
 *
 * @author Chris Cranford
 */
public class SkipTestDependingOnDatabaseVersionRule extends AnnotationBasedTestRule {

    @Override
    public Statement apply(Statement base, Description description) {

        // First check if multiple version skips are defined
        final SkipWhenDatabaseVersions skips = hasAnnotation(description, SkipWhenDatabaseVersions.class);
        if (skips != null) {
            final MySqlVersion dbVersion = MySQLConnection.forTestDatabase("mysql").getMySqlVersion();
            for (SkipWhenDatabaseVersion skip : skips.value()) {
                if (skip.version().equals(dbVersion)) {
                    String reasonForSkipping = "Database version is " + skip.version().name() + ": " + skip.reason();
                    return emptyStatement(reasonForSkipping, description);
                }
            }

            return base;
        }

        // Second check if a single version skip is defined
        final SkipWhenDatabaseVersion skip = hasAnnotation(description, SkipWhenDatabaseVersion.class);
        if (skip != null) {
            final MySqlVersion dbVersion = MySQLConnection.forTestDatabase("mysql").getMySqlVersion();
            if (skip.version().equals(dbVersion)) {
                String reasonForSkipping = "Database version is " + skip.version().name() + ": " + skip.reason();
                return emptyStatement(reasonForSkipping, description);
            }
        }

        return base;
    }
}
