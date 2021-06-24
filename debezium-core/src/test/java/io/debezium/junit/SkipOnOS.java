/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation used together with the {@link SkipTestRule} JUnit rule, that allows tests to be excluded
 * from the build if they are run on certain platforms.
 *
 * @author Horia Chiorean
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.TYPE })
public @interface SkipOnOS {

    /**
     * Symbolic constant used to determine the Windows operating system, from the "os.name" system property.
     */
    String WINDOWS = "windows";

    /**
     * Symbolic constant used to determine the OS X operating system, from the "os.name" system property.
     */
    String MAC = "mac";

    /**
     * Symbolic constant used to determine the Linux operating system, from the "os.name" system property.
     */
    String LINUX = "linux";

    /**
     * The list of OS names on which the test should be skipped.
     *
     * @return a list of "symbolic" OS names.
     * @see #WINDOWS
     * @see #MAC
     * @see #LINUX
     */
    String[] value();

    /**
     * An optional description which explains why the test should be skipped.
     *
     * @return a string which explains the reasons for skipping the test.
     */
    String description() default "";
}
