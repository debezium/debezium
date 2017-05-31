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
 * Master database instance used in tests verifying GTID functionality.
 * Is controlled manually.
 * 
 * @author Jiri Pechanec
 *
 */
@Retention(RUNTIME)
@Target(FIELD)
@CubeReference("database-gtids")
public @interface GtidsMasterDatabase {
}
