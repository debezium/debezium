/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.junit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation used together with the {@link SkipTestDependingOnGtidModeRule} JUnit rule, that allows
 * tests to be skipped based on the GTID mode set in the database used for testing
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.TYPE })
public @interface SkipWhenGtidModeIs {

    GtidMode value();

    /**
     * Returns the reason why the test should be skipped.
     */
    String reason() default "";

    enum GtidMode {
        ON,
        OFF
    }
}
