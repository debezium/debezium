/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service.spi;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.debezium.common.annotation.Incubating;

/**
 * Annotation allowing a service to request injection of other services.
 *
 * @author Chris Cranford
 */
@Incubating
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface InjectService {
    /**
     * The service that should be injected.
     * The default implementation is to use the argument type of the annotated method.
     *
     * @return the service class
     */
    Class<?> service() default Void.class;

    /**
     * Specifies whether the service injection is required.
     * The default implementation requires the service to be injected to avoid injection exceptions.
     *
     * @return {@code true} if the service is required, {@code false} if not.
     */
    boolean required() default true;
}
