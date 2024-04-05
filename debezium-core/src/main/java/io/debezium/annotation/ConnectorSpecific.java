/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.debezium.connector.common.BaseSourceConnector;

/**
 * Marks a class to be connector specific SPI implementation.
 * <p>
 * Used in combination with SPI loading mechanism where different implementation, of same interface,
 * is provided by different connectors.
 * Marking an implementation with this annotation will instruct {@link io.debezium.snapshot.SnapshotLockProvider} and {@link io.debezium.snapshot.SnapshotQueryProvider}
 * which implementation to load.
 */
@Documented
@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface ConnectorSpecific {

    Class<? extends BaseSourceConnector> connector();
}
