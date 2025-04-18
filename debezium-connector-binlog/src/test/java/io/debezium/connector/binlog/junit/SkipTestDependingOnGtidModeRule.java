/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.junit;

import java.sql.SQLException;

import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestConnectionService;
import io.debezium.junit.AnnotationBasedTestRule;

/**
 * JUnit rule that skips a test based on the {@link SkipWhenGtidModeIs} annotation on either a test method or a test class.
 */
public class SkipTestDependingOnGtidModeRule extends AnnotationBasedTestRule {
    private static final SkipWhenGtidModeIs.GtidMode gtidMode = getGtidMode();

    @Override
    public Statement apply(Statement base, Description description) {
        SkipWhenGtidModeIs skipGtidMode = hasAnnotation(description, SkipWhenGtidModeIs.class);
        if (skipGtidMode != null && skipGtidMode.value().equals(gtidMode)) {
            String reasonForSkipping = "GTID_MODE is " + skipGtidMode.value() + System.lineSeparator() + skipGtidMode.reason();
            return emptyStatement(reasonForSkipping, description);
        }
        return base;
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
