/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * JUnit rule that skips tests not intended for oplog capture mode.
 *
<<<<<<< HEAD
 * @author Jiri Pechanec
=======
 * @author Horia Chiorean
>>>>>>> DBZ-3342 Incremental snapshot support for MongoDB
 */
public class SkipForOplogTestRule implements TestRule {

    @Override
    public Statement apply(Statement base, Description description) {

        if (TestHelper.isOplogCaptureMode()) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    System.out.println("Skipped as test not supported for oplog capture mode");
                }
            };
        }

        return base;
    }
}
