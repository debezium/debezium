/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.junit;

import java.sql.SQLException;
import java.util.Objects;

import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.junit.AnnotationBasedTestRule;
import io.debezium.util.Strings;

/**
 * JUnit rule that automatically skips a test if the {@link SkipOnDatabaseOption} annotation at
 * either the class or test method level if the option specified isn't enabled or available on
 * the test database used by {@link TestHelper#testConnection}.
 *
 * @author Chris Cranford
 */
public class SkipTestDependingOnDatabaseOptionRule extends AnnotationBasedTestRule {
    private static final String FALSE = "FALSE";
    private static final String TRUE = "TRUE";

    @Override
    public Statement apply(Statement base, Description description) {
        final SkipOnDatabaseOption option = hasAnnotation(description, SkipOnDatabaseOption.class);
        if (Objects.nonNull(option)) {
            final String optionValue = getDatabaseOptionValue(option.value());
            if (option.enabled() && TRUE.equals(optionValue)) {
                return emptyStatement("Database option '" + optionValue + "' is enabled and available", description);
            }
            else if (!option.enabled() && (Strings.isNullOrEmpty(optionValue) || FALSE.equals(optionValue))) {
                return emptyStatement("Database option '" + optionValue + "' not available", description);
            }
        }
        return base;
    }

    private String getDatabaseOptionValue(String option) {
        try (OracleConnection connection = TestHelper.testConnection()) {
            return connection.queryAndMap("SELECT VALUE FROM V$OPTION WHERE PARAMETER='" + option + "'", (rs) -> {
                if (rs.next()) {
                    return rs.getString(1);
                }
                return null;
            });
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to resolve database option " + option, e);
        }
    }
}
