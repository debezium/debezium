/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.junit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.debezium.junit.SkipWhenDatabaseVersion;

/**
 * Marker annotation that allows controlling whether specific tests are invoked based on whether
 * the connection is to one database type or not; optionally specifying version constraints.
 *
 * While a test can be marked specifically with {@link SkipWhenDatabaseVersion} annotations, it
 * is desirable to use this annotation instead to influence the skip not only based on the
 * version but also the database type as MariaDB and MySQL both have different version schemes
 * that will eventually collide.
 *
 * Furthermore, the reason specified can be provided differently per database version, allowing
 * the test output to be as explicit as possible for why a given test is skipped as the reason
 * in the version takes precedence over the one at the database skip level. The database skip
 * reason will only be present if there are no version contraints or if all version constraits
 * provide no reason text.
 *
 * @author Chris Cranford
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.TYPE })
@Repeatable(SkipWhenDatabaseIsMultiple.class)
public @interface SkipWhenDatabaseIs {
    /**
     * Returns which database platform this skip exclusion applies
     */
    Type value();

    /**
     * Returns an array of database version constraints applied to skip rule
     */
    SkipWhenDatabaseVersion[] versions() default {};

    /**
     * Returns why the database is skipped. When specifying version constraints, the version
     * constraint reasons will have a priority over this reason when writing the skip reason
     * to the test output stream.
     */
    String reason() default "";

    enum Type {
        MYSQL,
        MARIADB,
        PERCONA
    }
}
