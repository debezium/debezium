/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.cube;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import io.debezium.connector.cube.CubeReference;

/**
 * Database instance used in most of tests. Should be controlled by Arquillian only.
 *
 * @author Jiri Pechanec
 *
 */
@Retention(RUNTIME)
@Target(FIELD)
@CubeReference("mysql-server")
public @interface DefaultDatabase {
}
