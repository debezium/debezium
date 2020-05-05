/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.format;

import io.debezium.common.annotation.Incubating;
import io.debezium.engine.SerializationFormat;

/**
 * Describes a change event output format comprising a key and a value.
 */
@Incubating
public interface KeyValueChangeEventFormat<K extends SerializationFormat<?>, V extends SerializationFormat<?>> extends ChangeEventFormat<V> {
    Class<K> getKeyFormat();
}
