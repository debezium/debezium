/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.junit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.debezium.connector.mysql.MySQLConnection.MySqlVersion;

/**
 * Marker annotation to control whether a test class or method is to be skipped based on the MySQL database version.
 *
 * @author Chris Cranford
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.METHOD })
@Repeatable(SkipWhenDatabaseVersions.class)
public @interface SkipWhenDatabaseVersion {
    /**
     * Specifies the version of MySQL for which the test method or class should be skipped.
     */
    MySqlVersion version();

    /**
     * Reason why the test is to be skipped.
     */
    String reason() default "";
}
