/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cube;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Creates a reference to a named Cube instance. The annotation takes a cube id as a parameter and can be applied
 * to a testcase field or to an annotation that is then applied to a testcase field
 * (similar mechanism to stereotypes in CDI).
 * 
 * @author Jiri Pechanec
 *
 */
@Retention(RUNTIME)
@Target({ ElementType.ANNOTATION_TYPE, ElementType.FIELD })
public @interface CubeReference {
    public String value();
}
