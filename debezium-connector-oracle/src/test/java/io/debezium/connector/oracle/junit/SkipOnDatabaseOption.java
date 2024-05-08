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
 * Marker annotation used to skip a given test if the specified database option is or isn't enabled.
 * based on whether {@code required} is set to true or false.
 *
 * @author Chris Cranford
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.TYPE })
public @interface SkipOnDatabaseOption {
    /**
     * Returns the database option that is inspected.  The option's name should
     * match the option name as defined by Oracle in the {@code V$OPTION} table.
     */
    String value();

    /**
     * Specifies whether the skip operation is based on if the option is enabled or disabled.
     */
    boolean enabled();

    /**
     * Specifies the reason why the test is being skipped.
     */
    String reason() default "";
}
