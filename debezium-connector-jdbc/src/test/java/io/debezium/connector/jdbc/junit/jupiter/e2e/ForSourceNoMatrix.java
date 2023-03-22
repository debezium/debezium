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
 * A marker meta annotation that explicitly skips all matrix bits and instead executes the
 * annotated method once only for the sources specified in the {@link #value()}.
 *
 * @author Chris Cranford
 */
@Target({ ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@SkipColumnTypePropagation
@SkipExtractNewRecordState
public @interface ForSourceNoMatrix {
    /**
     * Returns the connector type that will be used for the test template invocation matrix.
     */
    SourceType[] value() default {};

    /**
     * Returns the reason documenting why specific sources are excluded.
     */
    String reason() default "";
}
