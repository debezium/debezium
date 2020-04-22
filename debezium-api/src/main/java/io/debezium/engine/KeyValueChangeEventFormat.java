/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine;

import io.debezium.common.annotation.Incubating;

/**
 * The output format that is applicable separately to key and value.
 */
@Incubating
public interface KeyValueChangeEventFormat<T> extends ChangeEventFormat<T> {
}
