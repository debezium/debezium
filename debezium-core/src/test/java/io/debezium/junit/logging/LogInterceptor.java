/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit.logging;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Captures all logged messages, allowing us to verify log message was written.
 *
 * @author Gunnar Morling
 */
public final class LogInterceptor implements TestRule {

    private AssertingAppender interceptor;

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                interceptor = AssertingAppender.register();

                try {
                    base.evaluate();
                }
                finally {
                    interceptor.unregister();
                }
            }
        };
    }

    public boolean containsMessage(String message) {
        return interceptor.containsMessage(message);
    }

    public boolean containsWarnMessage(String message) {
        return interceptor.containsWarnMessage(message);
    }

    public boolean containsStacktraceElement(String text) {
        return interceptor.containsStacktraceElement(text);
    }
}
