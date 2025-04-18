/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A marker annotation that makes sure the specified PostgreSQL extension is installed before the method
 * is invoked and removes the extension after the method has completed.
 *
 * @author Chris Cranford
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD })
public @interface WithPostgresExtension {
    /**
     * Returns the name of the extension.
     */
    String value();
}
