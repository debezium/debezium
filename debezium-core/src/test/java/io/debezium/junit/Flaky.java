/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation used together with the {@link ConditionalFail} JUnit rule, that allows ignoring
 * failures of tests know to be unstable on CI, using the {@code ignoreFlakyFailures} system property.
 *
 * @author Jiri Pechanec
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.TYPE })
public @interface Flaky {

    String IGNORE_FLAKY_FAILURES_PROPERTY = "ignoreFlakyFailures";
    String FLAKY_ATTEMPTS_FAILURES_PROPERTY = "flaky.attempts";

    /**
     * The Jira id of the issue tracking the failing test
     */
    String value();
}
