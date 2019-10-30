/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.junit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation used together with the {@link SkipTestDependingOnDatabaseVersionRule} JUnit rule, that allows
 * tests to be skipped based on the Postgres version used for testing.
 *
 * @author Gunnar Morling
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.TYPE })
public @interface SkipWhenDatabaseVersionLessThan {

    PostgresVersion value();

    public enum PostgresVersion {
        POSTGRES_10 {
            @Override
            boolean isLargerThan(int otherMajor) {
                return otherMajor < 10;
            }
        };

        abstract boolean isLargerThan(int otherMajor);
    }
}
