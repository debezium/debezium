/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation used together with the {@link SkipTestRule} JUnit rule, that allows tests to be skipped
 * based on the current connector under test
 *
 * @author Jiri Pechanec
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.TYPE })
@Repeatable(SkipWhenConnectorsUnderTest.class)
public @interface SkipWhenConnectorUnderTest {

    Connector value();

    EqualityCheck check();

    String description() default "";

    enum Connector {
        SQL_SERVER {

            @Override
            boolean isEqualTo(String packageName) {
                return packageName != null && packageName.startsWith("io.debezium.connector.sqlserver");
            }
        },

        DB2 {

            @Override
            boolean isEqualTo(String packageName) {
                return packageName != null && packageName.startsWith("io.debezium.connector.db2");
            }
        };

        abstract boolean isEqualTo(String packageName);
    }
}
