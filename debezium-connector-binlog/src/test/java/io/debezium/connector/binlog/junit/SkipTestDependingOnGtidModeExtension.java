/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.junit;

import java.sql.SQLException;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestConnectionService;
import io.debezium.junit.AnnotationBasedExtension;

/**
 * JUnit 5 extension that skips a test based on the {@link SkipWhenGtidModeIs} annotation on either a test method or a test class.
 * This is the JUnit 5 equivalent of {@link SkipTestDependingOnGtidModeRule}.
 *
 * @author Chris Cranford
 */
public class SkipTestDependingOnGtidModeExtension extends AnnotationBasedExtension implements ExecutionCondition {

    private static final SkipWhenGtidModeIs.GtidMode gtidMode = getGtidMode();

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        SkipWhenGtidModeIs skipGtidMode = hasAnnotation(context, SkipWhenGtidModeIs.class);
        if (skipGtidMode != null && skipGtidMode.value().equals(gtidMode)) {
            String reasonForSkipping = "GTID_MODE is " + skipGtidMode.value() + System.lineSeparator() + skipGtidMode.reason();
            return ConditionEvaluationResult.disabled(reasonForSkipping);
        }
        return ConditionEvaluationResult.enabled("GTID_MODE is compatible");
    }

    public static SkipWhenGtidModeIs.GtidMode getGtidMode() {
        try (BinlogTestConnection db = TestConnectionService.forTestDatabase("emptydb")) {
            if (db.isMariaDb()) {
                // GTID mode is always enabled, and we shouldn't need to worry about GTID_STRICT_MODE
                return SkipWhenGtidModeIs.GtidMode.ON;
            }
            return db.queryAndMap(
                    "SHOW GLOBAL VARIABLES LIKE 'GTID_MODE'",
                    rs -> {
                        if (rs.next()) {
                            return SkipWhenGtidModeIs.GtidMode.valueOf(rs.getString(2));
                        }
                        throw new IllegalStateException("Cannot obtain GTID status");
                    });
        }
        catch (SQLException e) {
            throw new IllegalStateException("Cannot obtain GTID status", e);
        }
    }
}
