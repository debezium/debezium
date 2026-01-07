/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.junit;

import java.sql.SQLException;
import java.util.Objects;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.junit.AnnotationBasedExtension;
import io.debezium.util.Strings;

/**
 * JUnit 5 extension that automatically skips a test if the {@link SkipOnDatabaseOption} annotation at
 * either the class or test method level if the option specified isn't enabled or available on
 * the test database used by {@link TestHelper#testConnection}.
 *
 * @author Chris Cranford
 */
public class SkipTestDependingOnDatabaseOptionExtension extends AnnotationBasedExtension implements ExecutionCondition {
    private static final String FALSE = "FALSE";
    private static final String TRUE = "TRUE";

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        final SkipOnDatabaseOption option = hasAnnotation(context, SkipOnDatabaseOption.class);
        if (Objects.nonNull(option)) {
            final String optionValue = getDatabaseOptionValue(option.value());
            if (option.enabled() && TRUE.equals(optionValue)) {
                return ConditionEvaluationResult.disabled("Database option '" + optionValue + "' is enabled and available");
            }
            else if (!option.enabled() && (Strings.isNullOrEmpty(optionValue) || FALSE.equals(optionValue))) {
                return ConditionEvaluationResult.disabled("Database option '" + optionValue + "' not available");
            }
        }
        return ConditionEvaluationResult.enabled("Database option check passed");
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
