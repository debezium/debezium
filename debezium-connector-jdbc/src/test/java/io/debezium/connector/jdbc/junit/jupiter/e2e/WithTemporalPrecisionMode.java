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

import io.debezium.jdbc.TemporalPrecisionMode;

/**
 * A marker annotation that triggers the inclusion of the various {@link io.debezium.jdbc.TemporalPrecisionMode}
 * values as part of the test template's invocation matrix.
 *
 * By default, the annotation will run tests for all {@link TemporalPrecisionMode} values.
 *
 * If a test should only execute for a specific subset of modes, specify the {@link #include()} property
 * with the modes that should be tested. If a test should exclude a specific subset of modes, specify
 * the {@link #exclude()} property with the modes that should not be tested.
 *
 * The {@link #include()} and {@link #exclude()} should not be specified together, as this will result in
 * a test execution failure.
 *
 * @author Chris Cranford
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD })
public @interface WithTemporalPrecisionMode {
    /**
     * Specifies the temporal precision modes to include. By default, all modes are included.
     * This should not be specified when {@link #exclude()} is given.
     *
     * @return the precision modes to include
     */
    TemporalPrecisionMode[] include() default {};

    /**
     * Specifies the temporal precision modes to exclude. By default, none are excluded.
     * This should not be specified when {@link #include()} is given.
     *
     * @return the precision modes to exclude
     */
    TemporalPrecisionMode[] exclude() default {};
}
