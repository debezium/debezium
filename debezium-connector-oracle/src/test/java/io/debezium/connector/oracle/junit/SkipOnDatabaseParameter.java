/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.junit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation used to skip a given test based on the value of a database parameter.
 *
 * If the {@code matches} is set to {@code true}, skip if the parameter matches.
 * If the {@code matches} is set to {@code false}, skip if the parameter does not match.
 *
 * @author Chris Cranford
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.TYPE })
public @interface SkipOnDatabaseParameter {
    /**
     * Returns the parameter name to lookup.
     */
    String parameterName();

    /**
     * Returns the parameter value.
     */
    String value();

    /**
     * Whether the rule is based on it matching or not, defaults to {@code true}.
     */
    boolean matches() default true;

    /**
     * Specifies the reason why the test is skipped.
     */
    String reason() default "";
}
