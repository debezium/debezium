/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.reactive.spi;

public interface AsType<T> {

    /**
     * @return the Java type to which the event is converted
     */
    Class<T> getTargetType();
}
