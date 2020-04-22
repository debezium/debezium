/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine;

import io.debezium.common.annotation.Incubating;

/**
 * The output format that contains both key and value - like {@link SourceRecord}.
 */
@Incubating
public interface ContainerChangeEventFormat<T> extends ChangeEventFormat<T> {
}
