/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.testcontainers.containers.JdbcDatabaseContainer;

import io.debezium.connector.jdbc.junit.jupiter.e2e.SkipWhenSink;
import io.debezium.connector.jdbc.junit.jupiter.e2e.SkipWhenSinks;
import io.debezium.util.Strings;

/**
 * An abstract extension for providing a {@link Sink} parameter object to test methods.
 *
 * @author Chris Cranford
 */
public abstract class AbstractSinkDatabaseContextProvider implements BeforeAllCallback, AfterAllCallback, ParameterResolver, ExecutionCondition {

    private final SinkType sinkType;
    private final JdbcDatabaseContainer<?> container;
    private final Sink sink;

    public AbstractSinkDatabaseContextProvider(SinkType sinkType, JdbcDatabaseContainer<?> container) {
        this.sinkType = sinkType;
        this.container = container;
        this.sink = new Sink(sinkType, container);
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        container.start();
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        container.stop();
        sink.close();
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return parameterContext.getParameter().getType() == Sink.class;
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return sink;
    }

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        if (context.getTestMethod().isPresent()) {
            final SkipWhenSinks skipWhenSinks = context.getRequiredTestMethod().getAnnotation(SkipWhenSinks.class);
            if (skipWhenSinks != null) {
                for (SkipWhenSink skipWhenSink : skipWhenSinks.value()) {
                    if (isSkipped(skipWhenSink)) {
                        return getSkippedEvaluationResult(skipWhenSink);
                    }
                }
            }

            final SkipWhenSink skipWhenSink = context.getRequiredTestMethod().getAnnotation(SkipWhenSink.class);
            if (isSkipped(skipWhenSink)) {
                return getSkippedEvaluationResult(skipWhenSink);
            }
        }
        return ConditionEvaluationResult.enabled("Not annotated with SkipWhenSink for " + sinkType);
    }

    protected Sink getSink() {
        return sink;
    }

    private boolean isSkipped(SkipWhenSink skipWhenSink) {
        if (skipWhenSink != null) {
            for (SinkType sinkType : skipWhenSink.value()) {
                if (sinkType == this.sinkType) {
                    return true;
                }
            }
        }
        return false;
    }

    private ConditionEvaluationResult getSkippedEvaluationResult(SkipWhenSink skipWhenSink) {
        if (Strings.isNullOrBlank(skipWhenSink.reason())) {
            return ConditionEvaluationResult.disabled("Annotated with SkipWhenSink for " + sinkType);
        }
        return ConditionEvaluationResult.disabled("Skipped: " + skipWhenSink.reason());
    }

}
