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

import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Marker annotation that allows tests to be skipped based on the GTID mode set in the database used for testing.
 * This annotation automatically registers the {@link SkipTestDependingOnGtidModeExtension} to process the skip logic.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.TYPE })
@ExtendWith(SkipTestDependingOnGtidModeExtension.class)
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
