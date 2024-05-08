/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.function.Supplier;

/**
 * Marker annotation that defines if a test failure should be reported or not base upon condition passed to the annotation.
 * If the condition evaluates to <code>true</code> and a test fails then the failure is ignored. If the test does not fail
 * then a failure is reported.
 * If the condition evaluates to <code>false</code> then the test failure is handled in a usual way.
 *
 * @author Jiri Pechanec
 *
 */
@Retention(RUNTIME)
@Target({ METHOD, TYPE })
public @interface ShouldFailWhen {
    Class<? extends Supplier<Boolean>> value();
}
