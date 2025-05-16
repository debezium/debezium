/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.runtime;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.debezium.common.annotation.Incubating;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Incubating()
public @interface Capturing {

    String EVERYTHING_QUALIFIER = "*";

    /**
     *
     * @return the Fully qualified table names: https://debezium.io/documentation/reference/3.1/configuration/signalling.html#debezium-signaling-enabling-source-signaling-channel
     */
    String value() default EVERYTHING_QUALIFIER;
}
