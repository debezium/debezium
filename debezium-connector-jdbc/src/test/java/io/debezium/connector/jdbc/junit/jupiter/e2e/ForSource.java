/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter.e2e;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.debezium.connector.jdbc.junit.jupiter.e2e.source.SourceType;

/**
 * A marker annotation that can designate that a given test method should only be executed if
 * the test invocation's {@link SourceType} matches one of the entries in {@link #value()}.
 *
 * @author Chris Cranford
 */
@Target({ ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
public @interface ForSource {
    /**
     * Returns the connector type that will be used for the test template invocation matrix.
     */
    SourceType[] value() default {};

    /**
     * Returns the reason documenting why specific sources are excluded.
     */
    String reason() default "";
}
