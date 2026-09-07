/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.junit;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.junit.AnnotationBasedExtension;

/**
 * JUnit 5 extension that skips a test annotated with {@link SkipWhenSchemaHistoryDisabled} when the
 * testsuite runs with the binlog-metadata-based schema mode enabled, in which no schema history is used.
 */
public class SkipTestWhenSchemaHistoryDisabledExtension extends AnnotationBasedExtension implements ExecutionCondition {

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        SkipWhenSchemaHistoryDisabled annotation = hasAnnotation(context, SkipWhenSchemaHistoryDisabled.class);
        if (annotation != null && UniqueDatabase.isBinlogMetadataBasedSchemaTestsuite()) {
            String reasonForSkipping = "Test depends on the schema history, but the testsuite runs with the "
                    + "binlog-metadata-based schema mode" + System.lineSeparator() + annotation.reason();
            return ConditionEvaluationResult.disabled(reasonForSkipping);
        }
        return ConditionEvaluationResult.enabled("Schema history is in use");
    }
}
