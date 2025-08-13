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
import io.debezium.runtime.FieldFilterStrategy.DefaultFieldFilterStrategy;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Incubating()
public @interface CustomConverter {

    /**
     * @return the bean class instance that implements {@link FieldFilterStrategy}
     */
    Class<? extends FieldFilterStrategy> filter() default DefaultFieldFilterStrategy.class;
}
