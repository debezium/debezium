/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.junit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Marker annotation for allowing multiple {@link SkipWhenDatabaseIs} annotations.
 * This annotation automatically registers the {@link SkipTestDependingOnDatabaseExtension} to process the skip logic.
 *
 * @author Chris Cranford
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.METHOD })
@ExtendWith(SkipTestDependingOnDatabaseExtension.class)
public @interface SkipWhenDatabaseIsMultiple {
    /**
     * Specifies the constraints the test should be skipped for.
     */
    SkipWhenDatabaseIs[] value();
}
