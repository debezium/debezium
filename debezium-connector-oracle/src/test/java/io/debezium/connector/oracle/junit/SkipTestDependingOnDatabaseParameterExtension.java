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
 * JUnit 5 extension that automatically skips a test if the {@link SkipOnDatabaseParameter} annotation at
 * either the method or class evaluates to whether the test should be skipped.
 *
 * @author Chris Cranford
 */
public class SkipTestDependingOnDatabaseParameterExtension extends AnnotationBasedExtension implements ExecutionCondition {
    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        final SkipOnDatabaseParameter parameter = hasAnnotation(context, SkipOnDatabaseParameter.class);
        if (Objects.nonNull(parameter)) {
            Objects.requireNonNull(parameter.parameterName());
            final String databaseParameterValue = getDatabaseParameterValue(parameter.parameterName());
            if (parameter.matches() && Objects.equals(databaseParameterValue, parameter.value())) {
                return ConditionEvaluationResult.disabled(String.format("Database parameter: '%s' matches '%s'", parameter.parameterName(), parameter.value()));
            }
            else if (!parameter.matches() && !Objects.equals(databaseParameterValue, parameter.value())) {
                return ConditionEvaluationResult.disabled(String.format("Database parameter: '%s' does not match '%s'", parameter.parameterName(), parameter.value()));
            }
        }
        return ConditionEvaluationResult.enabled("Database parameter check passed");
    }

    private String getDatabaseParameterValue(String parameterName) {
        try (OracleConnection connection = TestHelper.adminConnection()) {
            // Connection must be in the container root if database supports pluggable databases
            if (!Strings.isNullOrBlank(connection.config().getString("pdb.name"))) {
                connection.resetSessionToCdb();
            }
            return connection.getDatabaseParameterValue(parameterName);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to resolve database parameter " + parameterName, e);
        }
    }
}
