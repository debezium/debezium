/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine;

import io.debezium.common.annotation.Incubating;

/**
 * A data change event described as a single object.
 *
 * @param <V>
 */
@Incubating
public interface RecordChangeEvent<V> {

    V record();
}
