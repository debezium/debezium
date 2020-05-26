/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import io.debezium.engine.format.SerializationFormat;

/**
 * A {@link SerializationFormat} defining the undefined serialization type.
 */
class Any implements SerializationFormat<Object> {
}
