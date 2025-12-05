/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation used together with the {@link SkipTestRule} JUnit rule, that allows tests to be skipped
 * based on the database version used for testing.
 *
 * @author Chris Cranford
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.TYPE })
@Repeatable(SkipWhenDatabaseVersions.class)
public @interface SkipWhenDatabaseVersion {

    /**
     * Specify the database major version; cannot be omitted.
     */
    int major();

    /**
     * Specify the database minor version; defaults to {@code -1} to imply skipping minor version check.
     */
    int minor() default -1;

    /**
     * Specify the database patch version; defaults to {code -1} to imply skipping patch version check.
     * @return
     */
    int patch() default -1;

    /**
     * Defines the type of equal-check that should initiate the skip on the test class or method.
     */
    EqualityCheck check();

    /**
     * Specifies the reason for skipping the method, used when logging a skipped test.
     */
    String reason();
}
