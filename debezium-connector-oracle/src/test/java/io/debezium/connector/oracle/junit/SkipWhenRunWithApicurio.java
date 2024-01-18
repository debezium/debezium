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
 * Marker annotation used together with {@link SkipTestWhenRunWithApicurioRule} JUnit rule, that allows
 * tests to not be skipped when the test is run against Apicurio registry.
 *
 * @author vjuranek
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.TYPE })
public @interface SkipWhenRunWithApicurio {
    String reason() default "";
}
