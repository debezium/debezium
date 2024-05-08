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
 * Marker annotation used together with the {@link SkipTestRule} JUnit rule, that allows long running tests to be excluded
 * from the build, using the {@code skipLongRunningTests} system property.
 *
 * @author Horia Chiorean
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.TYPE })
public @interface SkipLongRunning {

    String SKIP_LONG_RUNNING_PROPERTY = "skipLongRunningTests";

    /**
     * The optional reason why the test is skipped.
     * @return the reason why the test is skipped
     */
    String value() default "long-running";
}
