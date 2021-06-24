/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Denotes that the annotated element of a class that's meant for multi-threaded
 * usage is accessed only by single thread and thus doesn't need to be guarded
 * via synchronization or similar.
 *
 */
@Target({ ElementType.FIELD, ElementType.METHOD })
@Retention(RetentionPolicy.SOURCE)
public @interface SingleThreadAccess {

    /**
     * Describes the thread accessing the annotated element.
     */
    String value();
}
