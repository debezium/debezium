/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.junit;

import java.sql.SQLException;

import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import io.debezium.connector.postgresql.TestHelper;
import io.debezium.junit.AnnotationBasedTestRule;

/**
 * JUnit rule that skips a test based on the {@link SkipWhenDatabaseVersionLessThan} annotation either on a test method
 * or on a test class.
 *
 * @author Gunnar Morling
 */
public class SkipTestDependingOnDatabaseVersionRule extends AnnotationBasedTestRule {

    private static final int majorDbVersion = determineDbVersion();

    @Override
    public Statement apply(Statement base, Description description) {
        SkipWhenDatabaseVersionLessThan skip = hasAnnotation(description, SkipWhenDatabaseVersionLessThan.class);

        if (skip != null && skip.value().isLargerThan(majorDbVersion)) {
            String reasonForSkipping = "Database version less than " + skip.value();
            return emptyStatement(reasonForSkipping, description);
        }

        return base;
    }

    public static int determineDbVersion() {
        try {
            return TestHelper.create().connection().getMetaData().getDatabaseMajorVersion();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
