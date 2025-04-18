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

/**
 * Allows applying multiple {@link SkipWhenSource} annotations to the same method, allowing each
 * annotation to designate differ reasons why the sources are being skipped.
 *
 * @author Chris Cranford
 */
@Target({ ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
public @interface SkipWhenSources {
    SkipWhenSource[] value();
}
