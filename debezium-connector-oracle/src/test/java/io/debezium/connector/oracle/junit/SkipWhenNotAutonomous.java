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

import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Marker annotation that skips a test class unless the test database is an Oracle Autonomous
 * Database. The annotation is deliberately restricted to the class level; test classes that
 * require an Autonomous Database should be placed in the {@code io.debezium.connector.oracle.adb}
 * package and annotated as a whole.
 *
 * @author Chris Cranford
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@ExtendWith(SkipTestDependingOnAutonomousExtension.class)
public @interface SkipWhenNotAutonomous {

    /**
     * Returns the reason why the test should be skipped.
     */
    String reason() default "";
}