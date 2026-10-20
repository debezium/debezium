/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.junit;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.junit.AnnotationBasedExtension;

public class SkipTestOnPostgresVersionExtension extends AnnotationBasedExtension implements ExecutionCondition {

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(final ExtensionContext context) {
        final var annotation = hasAnnotation(context, SkipTestOnPostgresVersion.class);
        final var version = new PostgresDatabaseVersionResolver().getVersion();

        if (annotation.lessThan() >= 0 && version.isLessThan(annotation.lessThan(), 0, 0)) {
            return ConditionEvaluationResult.disabled("Postgres version is %d which is less than required %d"
                    .formatted(version.getMajor(), annotation.lessThan()));
        }

        if (annotation.greaterThan() >= 0 && version.isGreaterThan(annotation.greaterThan(), 0, 0)) {
            return ConditionEvaluationResult.disabled("Postgres version is %d which is greater than required %d"
                    .formatted(version.getMajor(), annotation.greaterThan()));
        }

        return ConditionEvaluationResult.enabled("Postgres version matches required version");
    }
}
