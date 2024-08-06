/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter.e2e;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.debezium.connector.jdbc.junit.jupiter.SinkType;

/**
 * Mark a test template method to be skipped if the current sink type matches any of the types
 * specific in the annotation's {@link #value()} array.
 *
 * @author Chris Cranford
 */
@Target({ ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(SkipWhenSinks.class)
public @interface SkipWhenSink {
    /**
     * Returns the connector types that will be excluded from the test template invocation matrix.
     */
    SinkType[] value() default {};

    /**
     * Returns the reason documenting why specific sinks are excluded.
     */
    String reason() default "";
}
