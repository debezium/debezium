/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.junit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.debezium.connector.mysql.MySqlConnectorConfig;

/**
 * Marker annotation used together with the {@link SkipTestDependingOnSslModeRule} JUnit rule, that allows
 * tests to be skipped based on the SSL mode set by system env used for testing
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.TYPE })
public @interface SkipWhenSslModeIsNot {

    MySqlConnectorConfig.SecureConnectionMode value();

    /**
     * Returns the reason why the test should be skipped.
     */
    String reason() default "";
}
